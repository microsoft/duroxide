// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Generic versioned registry for orchestrations and activities
//!
//! This module provides a unified `Registry<H>` type that can store both orchestration
//! and activity handlers with version support. Activities are always registered at
//! version 1.0.0 with Latest policy (hardcoded), while orchestrations support
//! explicit versioning and policies.

// Registry uses Mutex locks that should panic on poison (indicates serious corruption)
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]

use super::{ActivityHandler, FnActivity, FnOrchestration, OrchestrationHandler};
use crate::_typed_codec::Codec;
use crate::OrchestrationContext;
use semver::Version;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Default version for activities and default orchestration registrations
const DEFAULT_VERSION: Version = Version::new(1, 0, 0);

#[derive(Clone, Debug)]
pub enum VersionPolicy {
    Latest,
    Exact(Version),
}

/// Generic versioned registry
///
/// Both orchestrations and activities use this unified structure.
/// Activities are always stored at version 1.0.0 with Latest policy.
pub struct Registry<H: ?Sized> {
    pub(crate) inner: Arc<HashMap<String, std::collections::BTreeMap<Version, Arc<H>>>>,
    pub(crate) policy: Arc<Mutex<HashMap<String, VersionPolicy>>>,
}

// Manual Clone impl since H: ?Sized doesn't auto-derive Clone
impl<H: ?Sized> Clone for Registry<H> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            policy: Arc::clone(&self.policy),
        }
    }
}

impl<H: ?Sized> Default for Registry<H> {
    fn default() -> Self {
        Self {
            inner: Arc::new(HashMap::new()),
            policy: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// Generic registry builder
pub struct RegistryBuilder<H: ?Sized> {
    map: HashMap<String, std::collections::BTreeMap<Version, Arc<H>>>,
    policy: HashMap<String, VersionPolicy>,
    errors: Vec<String>,
}

// Type aliases for backward compatibility and clarity
pub type OrchestrationRegistry = Registry<dyn OrchestrationHandler>;
pub type ActivityRegistry = Registry<dyn ActivityHandler>;
pub type OrchestrationRegistryBuilder = RegistryBuilder<dyn OrchestrationHandler>;
pub type ActivityRegistryBuilder = RegistryBuilder<dyn ActivityHandler>;

// ============================================================================
// Generic Registry Implementation
// ============================================================================

impl<H: ?Sized> Registry<H> {
    pub fn builder() -> RegistryBuilder<H> {
        RegistryBuilder {
            map: HashMap::new(),
            policy: HashMap::new(),
            errors: Vec::new(),
        }
    }

    pub fn builder_from(reg: &Registry<H>) -> RegistryBuilder<H> {
        RegistryBuilder {
            map: reg.inner.as_ref().clone(),
            // Mutex lock should never fail in normal operation - if poisoned, it indicates a serious bug
            policy: reg.policy.lock().expect("Mutex should not be poisoned").clone(),
            errors: Vec::new(),
        }
    }

    /// Resolve handler using version policy (SYNC)
    pub fn resolve_handler(&self, name: &str) -> Option<(Version, Arc<H>)> {
        let pol = self
            .policy
            .lock()
            // Mutex lock should never fail in normal operation - if poisoned, it indicates a serious bug
            .expect("Mutex should not be poisoned")
            .get(name)
            .cloned()
            .unwrap_or(VersionPolicy::Latest);

        let result = match &pol {
            VersionPolicy::Latest => {
                if let Some(m) = self.inner.get(name) {
                    if let Some((v, h)) = m.iter().next_back() {
                        Some((v.clone(), h.clone()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            VersionPolicy::Exact(v) => self
                .inner
                .get(name)
                .and_then(|versions| versions.get(v))
                .map(|h| (v.clone(), Arc::clone(h))),
        };

        if result.is_none() {
            self.log_registry_miss(name, None, Some(&pol));
        }

        result
    }

    /// Resolve version using policy (SYNC)
    /// Note: This is primarily for testing. Production code should use `resolve_handler` which returns both version and handler.
    pub fn resolve_version(&self, name: &str) -> Option<Version> {
        self.resolve_handler(name).map(|(v, _h)| v)
    }

    /// Resolve handler for exact version (SYNC)
    pub fn resolve_handler_exact(&self, name: &str, v: &Version) -> Option<Arc<H>> {
        let result = if let Some(versions) = self.inner.get(name) {
            versions.get(v).cloned()
        } else {
            None
        };

        if result.is_none() {
            self.log_registry_miss(name, Some(v), None);
        }

        result
    }

    /// Set version policy (SYNC)
    pub fn set_version_policy(&self, name: &str, policy: VersionPolicy) {
        // Mutex lock should never fail in normal operation - if poisoned, it indicates a serious bug
        self.policy
            .lock()
            .expect("Mutex should not be poisoned")
            .insert(name.to_string(), policy);
    }

    /// List all registered names
    pub fn list_names(&self) -> Vec<String> {
        self.inner.keys().cloned().collect()
    }

    /// List versions for a specific name
    pub fn list_versions(&self, name: &str) -> Vec<Version> {
        self.inner
            .get(name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Check if a name is registered
    pub fn has(&self, name: &str) -> bool {
        self.inner.contains_key(name)
    }

    /// Count of registered handlers
    pub fn count(&self) -> usize {
        self.inner.len()
    }

    // Debug helpers
    fn debug_dump(&self) -> HashMap<String, Vec<String>> {
        self.inner
            .iter()
            .map(|(name, versions)| (name.clone(), versions.keys().map(|v| v.to_string()).collect()))
            .collect()
    }

    fn log_registry_miss(
        &self,
        name: &str,
        requested_version: Option<&Version>,
        requested_policy: Option<&VersionPolicy>,
    ) {
        let all_names = self.list_names();
        let contents = self.debug_dump();
        // Mutex lock should never fail in normal operation - if poisoned, it indicates a serious bug
        let policy_map = self.policy.lock().expect("Mutex should not be poisoned").clone();
        let available_versions = self.list_versions(name);

        tracing::debug!(
            target: "duroxide::runtime::registry",
            requested_name = %name,
            requested_version = ?requested_version,
            requested_policy = ?requested_policy,
            available_versions_for_name = ?available_versions,
            registered_count = all_names.len(),
            registered_names = ?all_names,
            full_registry_contents = ?contents,
            current_policies = ?policy_map,
            "Registry lookup miss - dumping full registry state"
        );
    }
}

// ============================================================================
// Generic Builder Implementation
// ============================================================================

impl<H: ?Sized> RegistryBuilder<H> {
    pub fn build(self) -> Registry<H> {
        Registry {
            inner: Arc::new(self.map),
            policy: Arc::new(Mutex::new(self.policy)),
        }
    }

    /// Build the registry, returning an error if there were any registration errors.
    ///
    /// # Errors
    ///
    /// Returns an error string containing all registration errors if any handlers failed to register.
    pub fn build_result(self) -> Result<Registry<H>, String> {
        if self.errors.is_empty() {
            Ok(self.build())
        } else {
            Err(self.errors.join("; "))
        }
    }

    /// Merge another registry into this builder (generic implementation)
    pub fn merge_registry(mut self, other: Registry<H>, error_prefix: &str) -> Self {
        for (name, versions) in other.inner.iter() {
            let entry = self.map.entry(name.clone()).or_default();
            for (version, handler) in versions.iter() {
                if entry.contains_key(version) {
                    self.errors
                        .push(format!("duplicate {error_prefix} in merge: {name}@{version}"));
                } else {
                    entry.insert(version.clone(), handler.clone());
                }
            }
        }
        self
    }

    /// Register multiple handlers at once (generic implementation)
    pub fn register_all_handlers<F>(self, items: Vec<(&str, F)>, register_fn: impl Fn(Self, &str, F) -> Self) -> Self
    where
        F: Clone,
    {
        items
            .into_iter()
            .fold(self, |builder, (name, f)| register_fn(builder, name, f))
    }

    /// Check for duplicate registration and return error if found
    fn check_duplicate(&mut self, name: &str, version: &Version, error_prefix: &str) -> bool {
        let entry = self.map.entry(name.to_string()).or_default();
        if entry.contains_key(version) {
            self.errors
                .push(format!("duplicate {error_prefix} registration: {name}@{version}"));
            true
        } else {
            false
        }
    }
}

// ============================================================================
// Orchestration Builder - Specialized Methods
// ============================================================================

impl OrchestrationRegistryBuilder {
    pub fn register<F, Fut>(mut self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(OrchestrationContext, String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        let name = name.into();
        if self.check_duplicate(&name, &DEFAULT_VERSION, "orchestration") {
            return self;
        }
        self.map
            .entry(name)
            .or_default()
            .insert(DEFAULT_VERSION, Arc::new(FnOrchestration(f)));
        self
    }

    pub fn register_typed<In, Out, F, Fut>(mut self, name: impl Into<String>, f: F) -> Self
    where
        In: serde::de::DeserializeOwned + Send + 'static,
        Out: serde::Serialize + Send + 'static,
        F: Fn(OrchestrationContext, In) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<Out, String>> + Send + 'static,
    {
        use super::FnOrchestration;
        let f_clone = f.clone();
        let wrapper = move |ctx: OrchestrationContext, input_s: String| {
            let f_inner = f_clone.clone();
            async move {
                let input: In = crate::_typed_codec::Json::decode(&input_s)?;
                let out: Out = f_inner(ctx, input).await?;
                crate::_typed_codec::Json::encode(&out)
            }
        };
        let name = name.into();
        self.map
            .entry(name)
            .or_default()
            .insert(DEFAULT_VERSION, Arc::new(FnOrchestration(wrapper)));
        self
    }

    pub fn register_versioned<F, Fut>(mut self, name: impl Into<String>, version: impl AsRef<str>, f: F) -> Self
    where
        F: Fn(OrchestrationContext, String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        let name = name.into();
        // Version parsing should never fail for valid semver strings from registry
        let v = Version::parse(version.as_ref()).expect("Version should be valid semver");
        if self.check_duplicate(&name, &v, "orchestration") {
            return self;
        }
        let entry = self.map.entry(name.clone()).or_default();
        if let Some((latest, _)) = entry.iter().next_back()
            && &v <= latest
        {
            panic!("non-monotonic orchestration version for {name}: {v} is not later than existing latest {latest}");
        }
        entry.insert(v, Arc::new(FnOrchestration(f)));
        self
    }

    pub fn register_versioned_typed<In, Out, F, Fut>(
        mut self,
        name: impl Into<String>,
        version: impl AsRef<str>,
        f: F,
    ) -> Self
    where
        In: serde::de::DeserializeOwned + Send + 'static,
        Out: serde::Serialize + Send + 'static,
        F: Fn(OrchestrationContext, In) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = Result<Out, String>> + Send + 'static,
    {
        use super::FnOrchestration;
        let name = name.into();
        // Version parsing should never fail for valid semver strings from registry
        let v = Version::parse(version.as_ref()).expect("Version should be valid semver");
        if self.check_duplicate(&name, &v, "orchestration") {
            return self;
        }
        let entry = self.map.entry(name.clone()).or_default();
        if let Some((latest, _)) = entry.iter().next_back()
            && &v <= latest
        {
            panic!("non-monotonic orchestration version for {name}: {v} is not later than existing latest {latest}");
        }
        let f_clone = f.clone();
        let wrapper = move |ctx: OrchestrationContext, input_s: String| {
            let f_inner = f_clone.clone();
            async move {
                let input: In = crate::_typed_codec::Json::decode(&input_s)?;
                let out: Out = f_inner(ctx, input).await?;
                crate::_typed_codec::Json::encode(&out)
            }
        };
        self.map
            .entry(name)
            .or_default()
            .insert(v, Arc::new(FnOrchestration(wrapper)));
        self
    }

    pub fn merge(self, other: OrchestrationRegistry) -> Self {
        self.merge_registry(other, "orchestration")
    }

    pub fn register_all<F, Fut>(self, items: Vec<(&str, F)>) -> Self
    where
        F: Fn(OrchestrationContext, String) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        self.register_all_handlers(items, |builder, name, f| builder.register(name, f))
    }

    pub fn set_policy(mut self, name: impl Into<String>, policy: VersionPolicy) -> Self {
        self.policy.insert(name.into(), policy);
        self
    }
}

// ============================================================================
// Activity Builder - Specialized Methods
// ============================================================================

impl ActivityRegistryBuilder {
    /// Deprecated: use `ActivityRegistry::builder_from` instead
    pub fn from_registry(reg: &ActivityRegistry) -> Self {
        ActivityRegistry::builder_from(reg)
    }

    /// Check if activity name uses a reserved prefix
    fn check_reserved_prefix(&mut self, name: &str) -> bool {
        if name.starts_with(crate::SYSCALL_ACTIVITY_PREFIX) {
            self.errors.push(format!(
                "activity name '{}' uses reserved prefix '{}'",
                name,
                crate::SYSCALL_ACTIVITY_PREFIX
            ));
            true
        } else {
            false
        }
    }

    pub fn register<F, Fut>(mut self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(crate::ActivityContext, String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        let name = name.into();
        // Check reserved prefix first
        if self.check_reserved_prefix(&name) {
            return self;
        }
        if self.check_duplicate(&name, &DEFAULT_VERSION, "activity") {
            return self;
        }
        self.map
            .entry(name.clone())
            .or_default()
            .insert(DEFAULT_VERSION, Arc::new(FnActivity(f)));
        // Set policy to Latest (hardcoded for activities)
        self.policy.insert(name, VersionPolicy::Latest);
        self
    }

    pub fn register_typed<In, Out, F, Fut>(mut self, name: impl Into<String>, f: F) -> Self
    where
        In: serde::de::DeserializeOwned + Send + 'static,
        Out: serde::Serialize + Send + 'static,
        F: Fn(crate::ActivityContext, In) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Out, String>> + Send + 'static,
    {
        let f_clone = std::sync::Arc::new(f);
        let wrapper = move |ctx: crate::ActivityContext, input_s: String| {
            let f_inner = f_clone.clone();
            async move {
                let input: In = crate::_typed_codec::Json::decode(&input_s)?;
                let out: Out = (f_inner)(ctx, input).await?;
                crate::_typed_codec::Json::encode(&out)
            }
        };
        let name = name.into();
        // Check reserved prefix first
        if self.check_reserved_prefix(&name) {
            return self;
        }
        if self.check_duplicate(&name, &DEFAULT_VERSION, "activity") {
            return self;
        }
        self.map
            .entry(name.clone())
            .or_default()
            .insert(DEFAULT_VERSION, Arc::new(FnActivity(wrapper)));
        // Set policy to Latest (hardcoded for activities)
        self.policy.insert(name, VersionPolicy::Latest);
        self
    }

    /// Register a built-in activity (bypasses reserved prefix check).
    /// Used internally to register system activities like new_guid and utc_now_ms.
    pub(crate) fn register_builtin<F, Fut>(mut self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(crate::ActivityContext, String) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        let name = name.into();
        // No reserved prefix check for builtins
        if self.check_duplicate(&name, &DEFAULT_VERSION, "activity") {
            return self;
        }
        self.map
            .entry(name.clone())
            .or_default()
            .insert(DEFAULT_VERSION, Arc::new(FnActivity(f)));
        self.policy.insert(name, VersionPolicy::Latest);
        self
    }

    pub fn merge(self, other: ActivityRegistry) -> Self {
        self.merge_registry(other, "activity")
    }

    pub fn register_all<F, Fut>(self, items: Vec<(&str, F)>) -> Self
    where
        F: Fn(crate::ActivityContext, String) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = Result<String, String>> + Send + 'static,
    {
        self.register_all_handlers(items, |builder, name, f| builder.register(name, f))
    }
}

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Scenario tests derived from real-world usage patterns
//!
//! This module contains regression tests that model specific scenarios found in actual
//! production usage of duroxide. These tests validate complex orchestration patterns,
//! long-running workflows, and edge cases discovered during real-world deployments.
//!
//! Examples include:
//! - Instance actor patterns with health checks
//! - Long continue-as-new chains
//! - Concurrent orchestration execution
//! - Complex activity workflows
//! - Single-thread runtime mode (for embedded hosts)
//! - Rolling deployment scenarios (multi-node with version upgrades)
//! - Unobserved future cancellation (select losers, dropped futures)
#![allow(clippy::unwrap_used)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::expect_used)]
// Allow duplicate module paths - scenarios share common module via #[path]
#![allow(clippy::duplicate_mod)]

#[path = "scenarios/toygres.rs"]
mod toygres;

#[path = "scenarios/single_thread.rs"]
mod single_thread;

#[path = "scenarios/rolling_deployment.rs"]
mod rolling_deployment;

#[path = "scenarios/version_replay_bug.rs"]
mod version_replay_bug;

#[path = "scenarios/sessions.rs"]
mod sessions;

#[cfg(feature = "replay-version-test")]
#[path = "scenarios/replay_versioning.rs"]
mod replay_versioning;

#[path = "scenarios/copilot_chat.rs"]
mod copilot_chat;

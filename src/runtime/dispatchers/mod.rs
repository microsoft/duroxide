// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Dispatcher implementations for Runtime
//!
//! This module contains the dispatcher logic split into separate concerns:
//! - `orchestration`: Orchestration dispatcher that processes orchestration turns
//! - `worker`: Worker dispatcher that executes activities

mod orchestration;
mod worker;

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Shared tracing capture helper for tests that need to assert on log output.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::{Dispatch, Event as TracingEvent, Level, Subscriber, dispatcher};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::{Context as LayerContext, Layer};
use tracing_subscriber::prelude::*;

#[derive(Debug, Clone)]
pub struct CapturedEvent {
    pub level: Level,
    pub target: String,
    pub message: String,
    pub fields: BTreeMap<String, String>,
}

impl CapturedEvent {
    /// Get a field value with surrounding quotes stripped.
    pub fn field(&self, key: &str) -> Option<String> {
        self.fields.get(key).map(|v| v.trim_matches('"').to_string())
    }
}

struct CaptureLayer {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

struct FieldCapture<'a> {
    fields: &'a mut BTreeMap<String, String>,
}

impl<'a> Visit for FieldCapture<'a> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.fields.insert(field.name().to_string(), value.to_string());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.fields.insert(field.name().to_string(), format!("{value:?}"));
    }
}

impl<S: Subscriber> Layer<S> for CaptureLayer {
    fn on_event(&self, event: &TracingEvent<'_>, _ctx: LayerContext<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut FieldCapture { fields: &mut fields });
        let meta = event.metadata();
        let message = fields.get("message").cloned().unwrap_or_default();
        self.events.lock().unwrap().push(CapturedEvent {
            level: *meta.level(),
            target: meta.target().to_string(),
            message,
            fields,
        });
    }
}

/// Install a tracing subscriber that captures all events.
///
/// Returns the captured events and a guard that must be held for the
/// duration of the test. When the guard is dropped the subscriber is
/// uninstalled.
pub fn install_tracing_capture() -> (Arc<Mutex<Vec<CapturedEvent>>>, dispatcher::DefaultGuard) {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let collector = tracing_subscriber::registry()
        .with(CaptureLayer {
            events: captured.clone(),
        })
        .with(LevelFilter::TRACE);
    let guard = dispatcher::set_default(&Dispatch::new(collector));
    (captured, guard)
}

//! Long polling validation tests for providers.
//!
//! These tests verify correct polling behavior. Providers that don't support long polling
//! should return immediately when no work is available. Providers that do support long polling
//! should block for up to the specified timeout.

use crate::providers::{Provider, TagFilter};
use std::time::{Duration, Instant};

/// Test for SHORT POLLING providers: Verify fetch returns immediately when no work exists.
///
/// Call this test for providers that DO NOT support long polling (e.g., SQLite).
/// The provider should ignore the `poll_timeout` and return `None` immediately.
pub async fn test_short_poll_returns_immediately(provider: &dyn Provider, threshold: Duration) {
    tracing::info!("→ Testing long polling: short poll returns immediately (threshold: {threshold:?})");

    let lock_timeout = Duration::from_secs(30);
    let poll_timeout = Duration::from_millis(500); // Provider should ignore this

    let start = Instant::now();
    let result = provider
        .fetch_orchestration_item(lock_timeout, poll_timeout, None)
        .await
        .expect("Fetch failed");
    let elapsed = start.elapsed();

    assert!(result.is_none(), "Queue should be empty");

    // Short polling provider should return much faster than poll_timeout
    assert!(
        elapsed < threshold,
        "Short polling provider should return immediately (elapsed: {elapsed:?}, threshold: {threshold:?})"
    );

    tracing::info!("✓ Short poll returned in {:?}", elapsed);
}

/// Test for LONG POLLING providers: Verify fetch blocks for the timeout period when no work exists.
///
/// Call this test for providers that DO support long polling.
/// The provider should block for approximately `poll_timeout` before returning `None`.
pub async fn test_long_poll_waits_for_timeout(provider: &dyn Provider) {
    tracing::info!("→ Testing long polling: long poll waits for timeout");

    let lock_timeout = Duration::from_secs(30);
    let poll_timeout = Duration::from_millis(500);

    let start = Instant::now();
    let result = provider
        .fetch_orchestration_item(lock_timeout, poll_timeout, None)
        .await
        .expect("Fetch failed");
    let elapsed = start.elapsed();

    assert!(result.is_none(), "Queue should be empty");

    // Long polling provider should wait for at least the timeout
    assert!(
        elapsed >= poll_timeout,
        "Long polling provider should wait for timeout (elapsed: {elapsed:?}, expected: {poll_timeout:?})"
    );

    tracing::info!("✓ Long poll waited {:?} (timeout was {:?})", elapsed, poll_timeout);
}

/// Universal test: Verify provider doesn't block EXCESSIVELY beyond the timeout.
///
/// This test applies to BOTH short and long polling providers. It verifies that
/// the provider respects the upper bound of the timeout (with some tolerance).
pub async fn test_fetch_respects_timeout_upper_bound(provider: &dyn Provider) {
    tracing::info!("→ Testing long polling: fetch respects timeout upper bound");

    let lock_timeout = Duration::from_secs(30);
    let poll_timeout = Duration::from_millis(300);
    let tolerance = Duration::from_millis(200); // Allow some slack for scheduling

    let start = Instant::now();
    let result = provider
        .fetch_orchestration_item(lock_timeout, poll_timeout, None)
        .await
        .expect("Fetch failed");
    let elapsed = start.elapsed();

    assert!(result.is_none(), "Queue should be empty");

    // Provider must not block excessively beyond timeout
    assert!(
        elapsed < poll_timeout + tolerance,
        "Provider blocked too long (elapsed: {:?}, max expected: {:?})",
        elapsed,
        poll_timeout + tolerance
    );

    tracing::info!("✓ Fetch completed in {:?} (within bounds)", elapsed);
}

/// Test for SHORT POLLING providers: Verify fetch_work_item returns immediately.
pub async fn test_short_poll_work_item_returns_immediately(provider: &dyn Provider, threshold: Duration) {
    tracing::info!("→ Testing long polling: short poll work item returns immediately (threshold: {threshold:?})");

    let lock_timeout = Duration::from_secs(30);
    let poll_timeout = Duration::from_millis(500);

    let start = Instant::now();
    let result = provider
        .fetch_work_item(lock_timeout, poll_timeout, None, &TagFilter::default())
        .await
        .expect("Fetch failed");
    let elapsed = start.elapsed();

    assert!(result.is_none(), "Worker queue should be empty");

    assert!(
        elapsed < threshold,
        "Short polling provider should return immediately (elapsed: {elapsed:?}, threshold: {threshold:?})"
    );

    tracing::info!("✓ Short poll work item returned in {:?}", elapsed);
}

/// Test for LONG POLLING providers: Verify fetch_work_item blocks for timeout.
pub async fn test_long_poll_work_item_waits_for_timeout(provider: &dyn Provider) {
    tracing::info!("→ Testing long polling: long poll work item waits for timeout");

    let lock_timeout = Duration::from_secs(30);
    let poll_timeout = Duration::from_millis(500);

    let start = Instant::now();
    let result = provider
        .fetch_work_item(lock_timeout, poll_timeout, None, &TagFilter::default())
        .await
        .expect("Fetch failed");
    let elapsed = start.elapsed();

    assert!(result.is_none(), "Worker queue should be empty");

    assert!(
        elapsed >= poll_timeout,
        "Long polling provider should wait for timeout (elapsed: {elapsed:?}, expected: {poll_timeout:?})"
    );

    tracing::info!("✓ Long poll work item waited {:?}", elapsed);
}

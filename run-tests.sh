#!/usr/bin/env bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Run the full duroxide test suite in two passes:
#   Pass 1: --all-features  (enables replay-version-test, runs v2 acceptance tests)
#   Pass 2: no features      (runs v1 serde rejection tests — proves v2 events are rejected)
#
# Usage:
#   ./run-tests.sh                      # both passes
#   ./run-tests.sh -E 'test(/pattern/)' # forward extra args to nextest
set -eo pipefail

echo "═══════════════════════════════════════════════════"
echo "  Pass 1: cargo nextest run --all-features"
echo "═══════════════════════════════════════════════════"
cargo nextest run --all-features "$@"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  Pass 2: cargo nextest run (no feature flags)"
echo "═══════════════════════════════════════════════════"
cargo nextest run "$@"

echo ""
echo "═══════════════════════════════════════════════════"
echo "  ✓ Both passes succeeded"
echo "═══════════════════════════════════════════════════"

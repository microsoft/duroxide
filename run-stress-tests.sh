#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Run Duroxide stress tests
#
# This script runs the stress test suite including:
# - Parallel orchestrations (fan-out/fan-in patterns)
# - Large payload (memory-intensive workloads)
#
# Usage:
#   ./run-stress-tests.sh [DURATION] [--track|--track-cloud] [--parallel-only|--large-payload]
#
# Examples:
#   ./run-stress-tests.sh                    # Run all tests for 10s with monitoring (default)
#   ./run-stress-tests.sh 60                 # Run all tests for 60 seconds
#   ./run-stress-tests.sh --parallel-only    # Run only parallel orchestrations test
#   ./run-stress-tests.sh --large-payload    # Run only large payload test
#   ./run-stress-tests.sh 5                  # Quick 5 second test (all tests)
#   ./run-stress-tests.sh 60 --track         # Run parallel for 60 seconds and track results
#   ./run-stress-tests.sh --track-cloud      # Track results to the cloud log

set -e

# Function to display help
show_help() {
    cat << 'EOF'
Duroxide Stress Test Suite

Run stress tests including parallel orchestrations (fan-out/fan-in patterns)
and large payload (memory-intensive workloads).

USAGE:
    ./run-stress-tests.sh [DURATION] [OPTIONS]

ARGUMENTS:
    DURATION    Test duration in seconds (default: 10)

OPTIONS:
    -h, --help          Show this help message and exit
    --parallel-only     Run only the parallel orchestrations test
    --large-payload     Run only the large payload test
    --track             Track results locally (parallel test only)
    --track-cloud       Track results to cloud log (parallel test only)

EXAMPLES:
    ./run-stress-tests.sh                    Run all tests for 10s (default)
    ./run-stress-tests.sh 60                 Run all tests for 60 seconds
    ./run-stress-tests.sh --parallel-only    Run only parallel orchestrations
    ./run-stress-tests.sh --large-payload    Run only large payload test
    ./run-stress-tests.sh 60 --track         Run parallel for 60s and track results
    ./run-stress-tests.sh --track-cloud      Track results to the cloud log
EOF
}

TRACK_MODE=""
DURATION="10"
TEST_TYPE="all"
MONITOR=true

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        -h|--help)
            show_help
            exit 0
            ;;
        --track)
            if [ "$TRACK_MODE" = "cloud" ]; then
                echo "Error: --track cannot be combined with --track-cloud" >&2
                exit 1
            fi
            TRACK_MODE="local"
            ;;
        --track-cloud)
            if [ "$TRACK_MODE" = "local" ]; then
                echo "Error: --track-cloud cannot be combined with --track" >&2
                exit 1
            fi
            TRACK_MODE="cloud"
            ;;
        --parallel-only)
            TEST_TYPE="parallel"
            ;;
        --large-payload)
            TEST_TYPE="large-payload"
            ;;
        *)

            if [[ "$arg" =~ ^[0-9]+$ ]]; then
                DURATION="$arg"
            else
                echo "Warning: Unrecognized argument '$arg' will be ignored" >&2
            fi
            ;;
    esac
done

echo "=========================================="
echo "Duroxide Stress Test Suite"
echo "=========================================="
if [ "$TEST_TYPE" = "all" ]; then
    echo "Test type: All tests (parallel + large payload)"
elif [ "$TEST_TYPE" = "large-payload" ]; then
    echo "Test type: Large payload"
else
    echo "Test type: Parallel orchestrations"
fi
echo "Duration: ${DURATION}s"
if [ "$MONITOR" = true ]; then
    echo "Monitoring: Enabled (RSS & CPU)"
else
    echo "Monitoring: Disabled"
fi
echo ""

# Function to monitor memory and CPU usage
monitor_process() {
    local pid=$1
    local interval=0.5  # Sample every 500ms
    local max_rss=0
    local total_cpu=0
    local samples=0

    while kill -0 $pid 2>/dev/null; do
        # Get RSS (KB) and CPU% on macOS using ps
        local ps_output=$(ps -o %cpu=,rss= -p $pid 2>/dev/null || echo "0 0")
        local cpu=$(echo "$ps_output" | awk '{print $1}')
        local rss=$(echo "$ps_output" | awk '{print $2}')

        if [ "$rss" -gt "$max_rss" ]; then
            max_rss=$rss
        fi

        total_cpu=$(echo "$total_cpu + $cpu" | bc 2>/dev/null || echo "$total_cpu")
        samples=$((samples + 1))

        sleep $interval
    done

    # Calculate averages
    local max_rss_mb=$((max_rss / 1024))
    local avg_cpu=$(echo "scale=2; $total_cpu / $samples" | bc 2>/dev/null || echo "0")

    echo ""
    echo "=========================================="
    echo "Resource Usage Metrics"
    echo "=========================================="
    echo "Peak RSS:     $max_rss_mb MB ($max_rss KB)"
    echo "Average CPU:  ${avg_cpu}% (of one core)"
    echo "Samples:      $samples"
    echo "=========================================="
}

if [ -n "$TRACK_MODE" ]; then
    if [ "$MONITOR" = true ]; then
        echo "Warning: Monitoring is not supported with --track mode"
        echo "Use without --track for memory/CPU metrics"
        echo ""
    fi

    if [ "$TRACK_MODE" = "cloud" ]; then
        echo "Running tests with cloud result tracking..."
    else
        echo "Running tests with result tracking..."
    fi
    echo ""
    # Run with tracking (pass duration if specified)
    # Note: tracking only supports parallel orchestrations test
    if [ "$TEST_TYPE" = "large-payload" ]; then
        echo "Warning: --track is not supported with --large-payload, running without tracking"
        cargo run --release --package duroxide-sqlite-stress --bin large-payload-stress "$DURATION"
    elif [ "$TEST_TYPE" = "all" ]; then
        echo "Warning: --track is not supported with --all, running only parallel orchestrations test"
        if [ "$TRACK_MODE" = "cloud" ]; then
            ./sqlite-stress/track-results.sh "$DURATION" --cloud
        else
            ./sqlite-stress/track-results.sh "$DURATION"
        fi
    else
        if [ "$TRACK_MODE" = "cloud" ]; then
            ./sqlite-stress/track-results.sh "$DURATION" --cloud
        else
            ./sqlite-stress/track-results.sh "$DURATION"
        fi
    fi
else
    # Run the stress tests in release mode for accurate performance metrics
    if [ "$TEST_TYPE" = "all" ]; then
        # Run both tests sequentially
        echo "Running all stress tests sequentially..."
        echo ""

        # Build both binaries first
        if [ "$MONITOR" = true ]; then
            echo "Building stress test binaries..."
            cargo build --release --package duroxide-sqlite-stress --bin sqlite-stress --quiet
            cargo build --release --package duroxide-sqlite-stress --bin large-payload-stress --quiet
        fi

        # Run parallel orchestrations test
        echo "=========================================="
        echo "Test 1/2: Parallel Orchestrations"
        echo "=========================================="
        if [ "$MONITOR" = true ]; then
            ./target/release/sqlite-stress "$DURATION" &
            TEST_PID=$!
            monitor_process $TEST_PID
            wait $TEST_PID
        else
            cargo run --release --package duroxide-sqlite-stress --bin sqlite-stress "$DURATION"
        fi

        echo ""
        echo "=========================================="
        echo "Test 2/2: Large Payload"
        echo "=========================================="

        # Run large payload test
        if [ "$MONITOR" = true ]; then
            ./target/release/large-payload-stress "$DURATION" &
            TEST_PID=$!
            monitor_process $TEST_PID
            wait $TEST_PID
        else
            cargo run --release --package duroxide-sqlite-stress --bin large-payload-stress "$DURATION"
        fi
    elif [ "$MONITOR" = true ]; then
        # Build first to avoid including build time in metrics
        if [ "$TEST_TYPE" = "large-payload" ]; then
            cargo build --release --package duroxide-sqlite-stress --bin large-payload-stress --quiet
            BIN_PATH="./target/release/large-payload-stress"
        else
            cargo build --release --package duroxide-sqlite-stress --bin sqlite-stress --quiet
            BIN_PATH="./target/release/sqlite-stress"
        fi

        # Run with monitoring
        $BIN_PATH "$DURATION" &
        TEST_PID=$!
        monitor_process $TEST_PID
        wait $TEST_PID
    else
        # Run without monitoring (normal mode)
        if [ "$TEST_TYPE" = "large-payload" ]; then
            cargo run --release --package duroxide-sqlite-stress --bin large-payload-stress "$DURATION"
        else
            cargo run --release --package duroxide-sqlite-stress --bin sqlite-stress "$DURATION"
        fi
    fi
fi

echo ""
echo "=========================================="
echo "Stress tests completed!"
echo "=========================================="


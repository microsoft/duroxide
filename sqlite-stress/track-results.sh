#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# Track stress test results with git history and rolling averages

set -e

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

RESULTS_FILE="stress-test-results.md"
GIT_LOG_PATTERN="%h %s"
CLOUD_MODE=false
DURATION=""

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        --cloud)
            CLOUD_MODE=true
            ;;
        *)
            if [[ "$arg" =~ ^[0-9]+$ ]]; then
                DURATION="$arg"
            else
                echo "Error: Unrecognized argument '$arg'" >&2
                exit 1
            fi
            ;;
    esac
done

if [ "$CLOUD_MODE" = true ]; then
    RESULTS_FILE="stress-test-results-cloud.md"
fi

# Get current commit hash and timestamp
CURRENT_COMMIT=$(git rev-parse --short HEAD)
TIMESTAMP=$(date -u +"%Y-%m-%d %H:%M:%S UTC")

# Get commit messages since last stress test
LAST_COMMIT=""
if [ -f "$RESULTS_FILE" ]; then
    # Extract the last commit hash from the results file
    LAST_COMMIT=$(grep -m 1 "^## Commit:" "$RESULTS_FILE" | cut -d' ' -f3 || echo "")
fi

if [ -z "$LAST_COMMIT" ]; then
    # First run - get all commits
    COMMIT_LOG=$(git log --pretty=format:"$GIT_LOG_PATTERN" -20)
else
    # Get commits since last stress test
    COMMIT_LOG=$(git log --pretty=format:"$GIT_LOG_PATTERN" ${LAST_COMMIT}..HEAD 2>/dev/null || echo "No commits since last test")
fi

# Run the stress tests and capture output
echo "Running stress tests..."

# Capture output to a temp file and then display it
TEMP_OUTPUT=$(mktemp)
if [ -n "$DURATION" ]; then
    cargo run --release --package duroxide-sqlite-stress --bin sqlite-stress "$DURATION" 2>&1 | tee "$TEMP_OUTPUT"
else
    cargo run --release --package duroxide-sqlite-stress --bin sqlite-stress 2>&1 | tee "$TEMP_OUTPUT"
fi
TEST_OUTPUT=$(cat "$TEMP_OUTPUT")
rm "$TEMP_OUTPUT"

# Extract comparison table from the output, stripping ANSI escape codes
# Find the table section and extract lines starting with "INFO" that contain the table
# Extract comparison table lines following the marker; accept lines with or without the INFO prefix
RESULTS=$(echo "$TEST_OUTPUT" \
  | awk '/=== Comparison Table ===/{found=1; next} found {print}' \
  | sed 's/\x1b\[[0-9;]*m//g' \
  | sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z[[:space:]]+INFO[[:space:]]+[a-zA-Z_:]+:[[:space:]]*//' \
  | sed -E 's/^[[:space:]]*INFO[[:space:]]+[a-zA-Z_:]+:[[:space:]]*//' \
  | sed -E 's/^[a-zA-Z_:]+:[[:space:]]*//' \
  | sed '/^$/d')

# Create the results entry
ENTRY_FILE=$(mktemp)
cat > "$ENTRY_FILE" << EOF

---

## Commit: $CURRENT_COMMIT - Timestamp: $TIMESTAMP

EOF

if [ "$CLOUD_MODE" = true ]; then
cat << 'EOF' >> "$ENTRY_FILE"
### Environment
- Cloud test environment

EOF
fi

cat << EOF >> "$ENTRY_FILE"
### Changes Since Last Test
\`\`\`
$COMMIT_LOG
\`\`\`

### Test Results
\`\`\`
$RESULTS
\`\`\`

EOF

# Prepend to results file
if [ -f "$RESULTS_FILE" ]; then
    # Read existing content, prepend new entry, and write back
    # Extract header if present, otherwise use default
    if grep -q "^# Duroxide Stress Test Results" "$RESULTS_FILE"; then
        # Keep header at top
        HEADER_LINES=$(grep -n "^# Duroxide Stress Test Results" "$RESULTS_FILE" | cut -d: -f1)
        HEADER_LINES=$((HEADER_LINES + 3))  # Include header and 2 blank lines after
        HEADER=$(head -n $HEADER_LINES "$RESULTS_FILE")
        CONTENT=$(tail -n +$((HEADER_LINES + 1)) "$RESULTS_FILE")
        (echo "$HEADER" && cat "$ENTRY_FILE" && echo "$CONTENT") > temp_results.md
    else
        # No header found, prepend entry
        (cat "$ENTRY_FILE" && cat "$RESULTS_FILE") > temp_results.md
    fi
    mv temp_results.md "$RESULTS_FILE"
else
    # Create new file with header
    if [ "$CLOUD_MODE" = true ]; then
        cat > "$RESULTS_FILE" << 'EOF'
# Duroxide Stress Test Results (Cloud)

<!-- Cloud environment runs. -->

EOF
    else
        cat > "$RESULTS_FILE" << 'EOF'
# Duroxide Stress Test Results

This file tracks all stress test runs, including performance metrics and commit changes.

EOF
    fi
    cat "$ENTRY_FILE" >> "$RESULTS_FILE"
fi

rm "$ENTRY_FILE"

echo ""
echo "=========================================="
if [ "$CLOUD_MODE" = true ]; then
    echo "Stress Test Results Tracked (Cloud)"
else
    echo "Stress Test Results Tracked"
fi
echo "=========================================="
echo "Commit: $CURRENT_COMMIT"
echo "Results saved to: $RESULTS_FILE"
echo ""
echo "Changes since last test:"
echo "$COMMIT_LOG"
echo ""
echo "Test Results:"
echo "$RESULTS"

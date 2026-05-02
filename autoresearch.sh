#!/bin/bash
# VerFSNext performance benchmark runner
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Fast build check (syntax errors)
cargo check --release 2>&1 | tail -5

# Full release build
cargo build --release 2>&1 | tail -5

# Run benchmark (full ComfyUI profile)
# The test outputs lines like "  summary_total          28977.9 ms"
VERFSNEXT_RUN_MOUNT_TESTS=1 cargo test bench_comfyui_profile --test rsync_integration -- --nocapture 2>&1 | tee /tmp/verfs_bench_out.txt

# Check if full benchmark ran (look for summary_total)
if grep -q 'summary_total' /tmp/verfs_bench_out.txt; then
    : # Full benchmark produced results
else
    echo "WARNING: Full benchmark did not produce output. Trying fast version."
    VERFSNEXT_RUN_MOUNT_TESTS=1 cargo test bench_comfyui_profile_fast --test rsync_integration -- --nocapture 2>&1 | tee /tmp/verfs_bench_out.txt
fi

# Parse test output which has format "  name                1234.5 ms"
# Also handle METRIC lines if test is updated to emit them directly
echo ""
echo "--- METRICS ---"
grep -E '^\s+[a-z_]+\s+[0-9.]+ ms' /tmp/verfs_bench_out.txt | while read -r line; do
    # Extract name (trim leading whitespace) and value
    name=$(echo "$line" | awk '{print $1}')
    value=$(echo "$line" | awk '{print $2}')
    # Remove trailing ' ms'
    value="${value% ms}"
    if [ -n "$name" ] && [ -n "$value" ]; then
        echo "METRIC ${name}=${value}"
    fi
done

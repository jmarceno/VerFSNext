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
# Output BENCH:name:nanoseconds lines parsed below into METRIC output
VERFSNEXT_RUN_MOUNT_TESTS=1 cargo test bench_comfyui_profile --test rsync_integration -- --nocapture 2>&1 | tee /tmp/verfs_bench_out.txt

# Check if test was actually run (not skipped)
if ! grep -q 'BENCH:' /tmp/verfs_bench_out.txt; then
    echo "WARNING: Full benchmark did not produce BENCH: lines. Trying fast version."
    VERFSNEXT_RUN_MOUNT_TESTS=1 cargo test bench_comfyui_profile_fast --test rsync_integration -- --nocapture 2>&1 | tee /tmp/verfs_bench_out.txt
fi

# Parse BENCH: lines and emit METRIC lines
echo ""
echo "--- METRICS ---"
grep '^BENCH:' /tmp/verfs_bench_out.txt | while IFS=: read -r _ name value_ns; do
    if [ -z "$name" ] || [ -z "$value_ns" ]; then
        continue
    fi
    # Convert nanoseconds to milliseconds (integer math, 1 decimal place)
    value_ms=$(( value_ns / 1000000 ))
    remainder=$(( (value_ns % 1000000) / 100000 ))
    echo "METRIC ${name}=${value_ms}.${remainder}"
done

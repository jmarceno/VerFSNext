#!/bin/bash
set -euo pipefail

# Fast benchmark for iteration: small files (30) + large files (4MB+8MB)
# Builds in release mode for accurate measurements.

export VERFSNEXT_RUN_MOUNT_TESTS=1

cargo build --release 2>&1 > /dev/null

output=$(cargo test bench_comfyui_profile_fast --test rsync_integration --release -- --nocapture 2>&1)

# Parse formatted lines from the benchmark:
#   "  sm_write_dura            108.2 ms"
#   "  summary_total           1338.5 ms"
while IFS= read -r line; do
    # Match lines starting with exactly two spaces, then a name, then numbers, then " ms"
    if [[ "$line" =~ ^[[:space:]]{2}([a-z_]+)[[:space:]]+([0-9]+\.?[0-9]*)[[:space:]]+ms$ ]]; then
        name="${BASH_REMATCH[1]}"
        val="${BASH_REMATCH[2]}"
        echo "METRIC ${name}=${val}"
    fi
done <<< "$output"

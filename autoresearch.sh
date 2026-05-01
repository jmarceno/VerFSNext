#!/bin/bash
set -euo pipefail

# Fast benchmark for iteration: small files (30) + large files (4MB+8MB)
# Outputs METRIC lines (primary + phase breakdown).

export VERFSNEXT_RUN_MOUNT_TESTS=1

output=$(cargo test bench_comfyui_profile_fast --test rsync_integration -- --nocapture 2>&1)

# Parse BENCH lines from output
while IFS= read -r line; do
    case "$line" in
        BENCH:*)
            rest="${line#BENCH:}"
            name="${rest%%:*}"
            ns="${rest##*:}"
            if [ -n "$ns" ] && [ "$ns" -eq "$ns" ] 2>/dev/null; then
                ms=$(echo "scale=1; $ns / 1000000" | bc)
                echo "METRIC ${name}=${ms}"
            fi
            ;;
    esac
done <<< "$output"

#!/usr/bin/env bash
# Runs a build command in a container on a copy of the checkout, then copies
# one output directory back.
#
# The job talks to the host's Docker daemon, which cannot see this job
# container's files, so bind mounts of the checkout would mount an empty or
# wrong host path. The tree goes in and out with `docker cp` instead; named
# volumes (passed as extra create args) still work for caches.
#
# Usage: container-build.sh <image> <out-dir> [docker-create-args...] -- <command...>
#   <out-dir> is relative to the checkout, both inside (/workspace) and here.
set -euo pipefail

image="${1:?image required}"
out_dir="${2:?output dir required}"
shift 2
create_args=()
while (($#)) && [[ "$1" != -- ]]; do
  create_args+=("$1")
  shift
done
[[ "${1:-}" == -- ]] || { echo "::error::container-build.sh: missing -- before the command" >&2; exit 2; }
shift
(($#)) || { echo "::error::container-build.sh: missing command" >&2; exit 2; }

cid="$(docker create -w /workspace "${create_args[@]}" "$image" "$@")"
trap 'docker rm -f "$cid" >/dev/null' EXIT
docker cp "$PWD/." "$cid:/workspace"
# Exits with the command's status.
docker start --attach "$cid"
rm -rf "$out_dir"
mkdir -p "$(dirname "$out_dir")"
docker cp "$cid:/workspace/$out_dir" "$out_dir"

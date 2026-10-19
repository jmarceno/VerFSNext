#!/usr/bin/env bash
# Writes the release version into a package's Cargo.toml and Cargo.lock so
# the built binary reports it (the change stays in the CI checkout).
#
# Usage: release-stamp-cargo.sh <Cargo.toml> <Cargo.lock> <package> <version>
set -euo pipefail

manifest="${1:?Cargo.toml path required}"
lock="${2:?Cargo.lock path required}"
package="${3:?package name required}"
version="${4:?version required}"

# The first top-level `version =` of the manifest belongs to [package].
sed -i -E '0,/^version[[:space:]]*=/s/^version[[:space:]]*=.*/version = "'"$version"'"/' "$manifest"
awk -v pkg="$package" -v ver="$version" '
  prev == "name = \"" pkg "\"" && /^version = / { $0 = "version = \"" ver "\"" }
  { print; prev = $0 }
' "$lock" >"$lock.new"
mv "$lock.new" "$lock"

grep -qx "version = \"$version\"" "$manifest" || { echo "::error::failed to stamp $manifest" >&2; exit 1; }
grep -A1 -x "name = \"$package\"" "$lock" | grep -qx "version = \"$version\"" \
  || { echo "::error::package $package not found in $lock" >&2; exit 1; }

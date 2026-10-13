#!/usr/bin/env bash
# Picks the version to release from the checked-out commit and writes
# `version`, `tag` and `notes` (path of a Markdown changelog) to
# $GITHUB_OUTPUT.
#
# The version is <manifest-version> when it is newer than every release tag,
# otherwise the newest release tag with its patch number bumped. A commit
# that already carries a release tag is refused: there is nothing new to ship.
#
# Usage: release-plan.sh <manifest-version>
set -euo pipefail

manifest="${1:?usage: release-plan.sh <manifest-version>}"
semver='^v?[0-9]+\.[0-9]+\.[0-9]+$'
[[ "$manifest" =~ $semver ]] || { echo "::error::manifest version '$manifest' is not X.Y.Z" >&2; exit 1; }
manifest="${manifest#v}"

git fetch --force --tags origin

released="$(git tag --points-at HEAD | grep -E "$semver" || true)"
if [[ -n "$released" ]]; then
  echo "::error::commit $(git rev-parse --short HEAD) is already released as ${released//$'\n'/, }" >&2
  exit 1
fi

latest=""
prev_tag=""
newest="$(git tag -l | grep -E "$semver" | awk '{ v = $0; sub(/^v/, "", v); print v, $0 }' | sort -V -k1,1 | tail -n1 || true)"
[[ -z "$newest" ]] || read -r latest prev_tag <<<"$newest"

if [[ -z "$latest" ]]; then
  version="$manifest"
elif [[ "$manifest" != "$latest" && "$(printf '%s\n%s\n' "$latest" "$manifest" | sort -V | tail -n1)" == "$manifest" ]]; then
  version="$manifest"
else
  IFS=. read -r major minor patch <<<"$latest"
  version="$major.$minor.$((patch + 1))"
fi

notes="$RUNNER_TEMP/release-notes.md"
range=HEAD
[[ -z "$prev_tag" ]] || range="$prev_tag..HEAD"
{
  if [[ -n "$prev_tag" ]]; then echo "## Changes since $prev_tag"; else echo "## Changes"; fi
  echo
  git log --no-merges --pretty='- %s (%h)' "$range" | head -n 300
} >"$notes"

echo "Releasing v$version (manifest $manifest, previous ${prev_tag:-none})"
{
  echo "version=$version"
  echo "tag=v$version"
  echo "notes=$notes"
} >>"$GITHUB_OUTPUT"

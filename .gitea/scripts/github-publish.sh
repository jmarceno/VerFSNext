#!/usr/bin/env bash
# Copies a release to GitHub: same tag on the same commit, same title, notes
# and assets. Safe to re-run: an existing GitHub release for <tag> is reused
# and only the assets it lacks are uploaded.
#
# Env:   GH_REPO (owner/name), GH_TOKEN, RELEASE_SHA
# Usage: github-publish.sh <tag> <title> <notes-file> <dist-dir>
set -euo pipefail

tag="${1:?tag required}"
title="${2:?title required}"
notes="${3:?notes file required}"
dist="${4:?dist dir required}"
: "${GH_REPO:?}" "${GH_TOKEN:?GH_TOKEN is empty: set the GH_RELEASE_TOKEN Actions secret}" "${RELEASE_SHA:?}"

api="https://api.github.com/repos/$GH_REPO"
curl_config="$(mktemp)"
response="$(mktemp)"
trap 'rm -f "$curl_config" "$response"' EXIT
chmod 0600 "$curl_config"
{
  printf 'header = "Authorization: Bearer %s"\n' "$GH_TOKEN"
  printf 'header = "Accept: application/vnd.github+json"\n'
  printf 'header = "X-GitHub-Api-Version: 2022-11-28"\n'
} >"$curl_config"
gh_api() { curl --fail-with-body --silent --show-error --config "$curl_config" "$@"; }

code="$(curl --silent --config "$curl_config" -o "$response" -w '%{http_code}' "$api/releases/tags/$tag")"
case "$code" in
  200)
    echo "GitHub release $tag exists; uploading missing assets only"
    release="$(cat "$response")"
    ;;
  404)
    payload="$(jq -n --arg tag "$tag" --arg sha "$RELEASE_SHA" --arg name "$title" --rawfile body "$notes" \
      '{tag_name: $tag, target_commitish: $sha, name: $name, body: $body, draft: false, prerelease: false}')"
    release="$(gh_api -H 'Content-Type: application/json' -d "$payload" "$api/releases")"
    ;;
  *)
    echo "::error::GitHub release lookup for $tag failed with HTTP $code: $(cat "$response")" >&2
    exit 1
    ;;
esac

upload_url="$(jq -er '.upload_url' <<<"$release")"
upload_url="${upload_url%%\{*}"
present="$(jq -r '.assets[].name' <<<"$release")"

for asset in "$dist"/*; do
  [[ -f "$asset" ]] || { echo "::error::not a file: $asset" >&2; exit 1; }
  name="$(basename "$asset")"
  if grep -qxF "$name" <<<"$present"; then
    echo "Already on GitHub: $name"
    continue
  fi
  gh_api -H 'Content-Type: application/octet-stream' --data-binary "@$asset" \
    "$upload_url?name=$(jq -rn --arg n "$name" '$n | @uri')" >/dev/null
  echo "Uploaded $name to GitHub ($(du -h "$asset" | cut -f1))"
done

echo "GitHub release: $(jq -r '.html_url' <<<"$release")"

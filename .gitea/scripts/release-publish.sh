#!/usr/bin/env bash
# Publishes every file in <dist-dir> as release <tag> on $RELEASE_SHA.
#
# The release is created as a draft, filled, then published: a failed
# upload never leaves a half-published version, and Gitea creates the tag
# only when the draft is published. Drafts left for <tag> by an earlier
# failed run are removed first.
#
# Env:   GITEA_API (…/api/v1/repos/<owner>/<repo>), GITEA_TOKEN, RELEASE_SHA
# Usage: release-publish.sh <tag> <title> <notes-file> <dist-dir>
set -euo pipefail

tag="${1:?tag required}"
title="${2:?title required}"
notes="${3:?notes file required}"
dist="${4:?dist dir required}"
: "${GITEA_API:?}" "${GITEA_TOKEN:?}" "${RELEASE_SHA:?}"

curl_config="$(mktemp)"
trap 'rm -f "$curl_config"' EXIT
chmod 0600 "$curl_config"
printf 'header = "Authorization: token %s"\n' "$GITEA_TOKEN" >"$curl_config"
api() { curl --fail-with-body --silent --show-error --config "$curl_config" "$@"; }

existing="$(curl --silent --config "$curl_config" -o /dev/null -w '%{http_code}' "$GITEA_API/releases/tags/$tag")"
[[ "$existing" == 404 ]] || { echo "::error::release $tag already exists (HTTP $existing)" >&2; exit 1; }

api "$GITEA_API/releases?draft=true&limit=50" \
  | jq -r --arg tag "$tag" '.[] | select(.draft and .tag_name == $tag) | .id' \
  | while read -r stale; do
      echo "Removing stale draft $stale for $tag"
      api -X DELETE "$GITEA_API/releases/$stale"
    done

payload="$(jq -n --arg tag "$tag" --arg sha "$RELEASE_SHA" --arg name "$title" --rawfile body "$notes" \
  '{tag_name: $tag, target_commitish: $sha, name: $name, body: $body, draft: true, prerelease: false}')"
id="$(api -H 'Content-Type: application/json' -d "$payload" "$GITEA_API/releases" | jq -er .id)"

for asset in "$dist"/*; do
  [[ -f "$asset" ]] || { echo "::error::not a file: $asset" >&2; exit 1; }
  name="$(basename "$asset")"
  api -F "attachment=@$asset" "$GITEA_API/releases/$id/assets?name=$(jq -rn --arg n "$name" '$n | @uri')" >/dev/null
  echo "Uploaded $name ($(du -h "$asset" | cut -f1))"
done

url="$(api -X PATCH -H 'Content-Type: application/json' -d '{"draft": false}' "$GITEA_API/releases/$id" | jq -er .html_url)"
echo "Published $tag: $url"

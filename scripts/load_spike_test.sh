#!/usr/bin/env bash
# Load-spike smoke test: fires N concurrent video uploads at the running
# local stack (see deploy/local/docker-compose.yml) and proves the system
# accepts every one of them and eventually finishes every one of them, even
# though the processing-service worker consumes its queue one message at a
# time (no consumer concurrency) — the burst is absorbed by the outbox +
# queue, not by rejecting requests. See project-5-hacka/docs/runbook.md for
# the equivalent single-request golden path this stresses concurrently.
#
# Usage: scripts/load_spike_test.sh [N] [UPLOAD_BASE_URL]
#   N                default 30   number of concurrent uploads to fire
#   UPLOAD_BASE_URL  default http://localhost:8081

set -euo pipefail

N="${1:-30}"
BASE_URL="${2:-http://localhost:8081}"
case "$N" in ''|*[!0-9]*|0) echo "N must be a positive integer, got: $N" >&2; exit 1 ;; esac
FIXTURE="${FIXTURE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../fiapx-video-processing-service" && pwd)/tests/fixtures/sample.mp4}"
POLL_TIMEOUT_S="${POLL_TIMEOUT_S:-180}"
POLL_INTERVAL_S="${POLL_INTERVAL_S:-2}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

for cmd in curl jq; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "missing required command: $cmd" >&2; exit 1; }
done
if [ ! -f "$FIXTURE" ]; then
  echo "fixture not found: $FIXTURE (see tests/fixtures/README.md in fiapx-video-processing-service)" >&2
  exit 1
fi

email="load-spike-$(date +%s)-$$@fiapx.local"
password="senha123"

echo "== registering $email =="
curl -sS -o /dev/null -w '%{http_code}\n' -X POST "$BASE_URL/api/v1/auth/register" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$email\",\"password\":\"$password\"}"

token="$(curl -sS -X POST "$BASE_URL/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"$email\",\"password\":\"$password\"}" | jq -r .token)"
if [ -z "$token" ] || [ "$token" = "null" ]; then
  echo "login failed, no token" >&2
  exit 1
fi

echo "== firing $N concurrent uploads against $BASE_URL =="
start_ts=$(date +%s)
for i in $(seq 1 "$N"); do
  (
    resp="$(curl -sS -w '\n%{http_code}' -X POST "$BASE_URL/api/v1/videos" \
      -H "Authorization: Bearer $token" \
      -F "video=@$FIXTURE;filename=spike-$i.mp4")"
    code="$(tail -n1 <<<"$resp")"
    body="$(sed '$d' <<<"$resp")"
    video_id="$(jq -r '.video_id // empty' <<<"$body" 2>/dev/null || true)"
    printf '%s\t%s\t%s\n' "$i" "$code" "$video_id" > "$WORKDIR/req-$i.result"
  ) &
done
wait
upload_elapsed=$(( $(date +%s) - start_ts ))

accepted=0
rejected=0
video_ids=()
while IFS=$'\t' read -r i code vid; do
  if [ "$code" = "202" ] && [ -n "$vid" ]; then
    accepted=$((accepted + 1))
    video_ids+=("$vid")
  else
    rejected=$((rejected + 1))
    echo "  request $i NOT accepted: HTTP $code" >&2
  fi
done < <(cat "$WORKDIR"/req-*.result)

echo "== upload burst done in ${upload_elapsed}s: $accepted/$N accepted, $rejected rejected =="
if [ "$accepted" -ne "$N" ]; then
  echo "FAIL: expected all $N requests to be accepted (202), only $accepted were" >&2
  exit 1
fi

echo "== polling until all $accepted videos reach a terminal state (timeout ${POLL_TIMEOUT_S}s) =="
deadline=$(( $(date +%s) + POLL_TIMEOUT_S ))
completed=0
failed=0
pending_file="$WORKDIR/pending.txt"
printf '%s\n' "${video_ids[@]}" > "$pending_file"
while [ -s "$pending_file" ] && [ "$(date +%s)" -lt "$deadline" ]; do
  # GET /api/v1/videos defaults to a page size of 20 when `limit` is
  # omitted; without this, a burst larger than 20 would leave older videos
  # permanently off the first page and look "stuck" here even after they
  # actually complete.
  listing="$(curl -sS "$BASE_URL/api/v1/videos?limit=$N" -H "Authorization: Bearer $token")"
  still_pending_file="$WORKDIR/still_pending.txt"
  : > "$still_pending_file"
  while IFS= read -r vid; do
    [ -z "$vid" ] && continue
    status="$(jq -r --arg id "$vid" '.videos[] | select(.video_id == $id) | .status' <<<"$listing")"
    case "$status" in
      COMPLETED) completed=$((completed + 1)) ;;
      FAILED)    failed=$((failed + 1)) ;;
      *)         echo "$vid" >> "$still_pending_file" ;;
    esac
  done < "$pending_file"
  mv "$still_pending_file" "$pending_file"
  [ -s "$pending_file" ] && sleep "$POLL_INTERVAL_S"
done
process_elapsed=$(( $(date +%s) - start_ts ))
pending_count=$(grep -c . "$pending_file" 2>/dev/null || true)
pending_count="${pending_count:-0}"

echo "== result: $completed completed, $failed failed, $pending_count still pending after ${process_elapsed}s total =="
if [ "$pending_count" -gt 0 ]; then
  echo "FAIL: $pending_count video(s) never reached a terminal state within ${POLL_TIMEOUT_S}s:" >&2
  cat "$pending_file" >&2
  exit 1
fi
if [ "$failed" -gt 0 ]; then
  echo "FAIL: $failed video(s) reached FAILED (fixture is a valid video, none were expected to fail)" >&2
  exit 1
fi

echo "PASS: all $N concurrent uploads were accepted and all $completed completed successfully."
echo "      accepted-all-requests: ${upload_elapsed}s, drained-full-queue: ${process_elapsed}s"

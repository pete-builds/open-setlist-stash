#!/usr/bin/env bash
# Smoke test for the one-command demo (docker-compose.demo.yml).
#
# Starts the demo, waits for it to be healthy, then drives it over plain HTTP
# the way a visitor would: home page, pick a handle, open the upcoming show,
# search a song, submit picks, and check the seeded leaderboard. Exits non-zero
# on the first thing that does not look right, after printing the compose logs.
#
# CI runs exactly this (see the demo-smoke job in .github/workflows/ci.yml).
# Locally:
#   bash demo/smoke_test.sh               # build, start, check (leaves it running)
#   SMOKE_SKIP_UP=1 bash demo/smoke_test.sh   # check an already-running demo
#   docker compose -f docker-compose.demo.yml down -v   # tear down
#
# Works on Linux and macOS (bash 3.2+, curl, grep, sed).

set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker-compose.demo.yml)
PORT="${DEMO_PORT:-8706}"
BASE="http://127.0.0.1:${PORT}"
WAIT_SECONDS="${SMOKE_WAIT_SECONDS:-300}"
WORK="$(mktemp -d)"
JAR="${WORK}/cookies.txt"
trap 'rm -rf "${WORK}"' EXIT

fail() {
  echo "SMOKE FAIL: $*" >&2
  "${COMPOSE[@]}" ps -a >&2 || true
  "${COMPOSE[@]}" logs --no-color --tail=80 >&2 || true
  exit 1
}

pass() {
  echo "ok   $*"
}

# fetch <name> <curl args...>: body to $WORK/<name>.html, prints the HTTP status.
fetch() {
  local name="$1"
  shift
  curl -sS -o "${WORK}/${name}.html" -w '%{http_code}' -b "${JAR}" -c "${JAR}" "$@" \
    || echo "000"
}

expect_in() {
  local name="$1" needle="$2"
  grep -q -- "${needle}" "${WORK}/${name}.html" \
    || fail "${name}: expected to find '${needle}'"
}

start=$(date +%s)

if [ "${SMOKE_SKIP_UP:-0}" != "1" ]; then
  "${COMPOSE[@]}" up -d --build
fi

# 1. Wait for /healthz to report the database and the data server reachable.
deadline=$(( $(date +%s) + WAIT_SECONDS ))
healthy=0
while [ "$(date +%s)" -lt "${deadline}" ]; do
  code=$(fetch healthz "${BASE}/healthz")
  if [ "${code}" = "200" ] \
    && grep -q '"db":{"reachable":true}' "${WORK}/healthz.html" \
    && grep -q '"mcp_phish":{"reachable":true' "${WORK}/healthz.html"; then
    healthy=1
    break
  fi
  states=$("${COMPOSE[@]}" ps -a --format '{{.Service}} {{.State}} {{.ExitCode}}' 2>/dev/null \
    || true)
  if grep -q '^demo-seed exited [1-9]' <<<"${states}"; then
    fail "the demo-seed step exited non-zero"
  fi
  sleep 2
done
[ "${healthy}" = "1" ] || fail "no healthy /healthz within ${WAIT_SECONDS}s"
ready=$(( $(date +%s) - start ))
pass "healthz: db and data server reachable (${ready}s after start)"

# 2. Home page, anonymous: the demo notice and the handle form render.
code=$(fetch home "${BASE}/")
[ "${code}" = "200" ] || fail "GET / returned ${code}"
expect_in home "made-up band"
expect_in home 'action="/handle"'
pass "home page renders for an anonymous visitor"

# 3. Pick an anonymous handle (the local-only sign-in this app supports).
handle="smoke_$(date +%s)"
code=$(fetch handle -X POST --data-urlencode "handle=${handle}" "${BASE}/handle")
[ "${code}" = "303" ] || fail "POST /handle returned ${code}"
pass "anonymous handle ${handle} created"

# 4. Home page, signed in: it links the upcoming show's pick sheet.
code=$(fetch home_in "${BASE}/")
[ "${code}" = "200" ] || fail "GET / (signed in) returned ${code}"
link=$(grep -o -m 1 'href="/predict/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}"' \
  "${WORK}/home_in.html" || true)
[ -n "${link}" ] || fail "home page has no link to an upcoming show's pick sheet"
show_date="${link#href=\"/predict/}"
show_date="${show_date%\"}"
today=$(date -u +%Y-%m-%d)
# Lexical compare works for ISO dates; the demo never schedules today/tomorrow.
[[ "${show_date}" > "${today}" ]] || fail "upcoming show ${show_date} is not in the future"
pass "home page links the upcoming show (${show_date})"

# 5. The upcoming show's pick sheet: venue from the data server, open form.
code=$(fetch predict "${BASE}/predict/${show_date}")
[ "${code}" = "200" ] || fail "GET /predict/${show_date} returned ${code}"
expect_in predict "Fictional Fieldhouse"
expect_in predict 'id="predict-form"'
pass "pick sheet for ${show_date} is open"

# 6. Song search, as the picker calls it.
code=$(fetch search "${BASE}/songs/search?q=comet")
[ "${code}" = "200" ] || fail "GET /songs/search returned ${code}"
expect_in search 'value="cardboard-comet"'
pass "song search finds Cardboard Comet"

# 7. Submit picks through the form endpoint.
code=$(fetch submit -X POST \
  --data-urlencode "pick_1=cardboard-comet" \
  --data-urlencode "pick_2=moth-radio" \
  --data-urlencode "pick_3=velvet-weather" \
  --data-urlencode "encore_pick=pick_1" \
  "${BASE}/predict/${show_date}")
[ "${code}" = "200" ] || fail "POST /predict/${show_date} returned ${code}"
expect_in submit "Picks locked in"
pass "picks submitted"

# 8. The picks persisted: reopening the sheet pre-fills them.
code=$(fetch predict_again "${BASE}/predict/${show_date}")
[ "${code}" = "200" ] || fail "GET /predict/${show_date} (again) returned ${code}"
expect_in predict_again 'value="moth-radio"'
pass "submitted picks are saved"

# 9. The seeded, scored past shows put players on the leaderboard.
code=$(fetch board "${BASE}/leaderboard?scope=all_time")
[ "${code}" = "200" ] || fail "GET /leaderboard returned ${code}"
expect_in board "demo_dana"
pass "leaderboard shows the seeded demo players"

# 10. The demo runs on the app's public development session key. The app must
# refuse to boot with it once BASE_URL is https, or this file could be turned
# into a public deployment with forgeable sessions by changing one variable.
cid=$("${COMPOSE[@]}" run -d --no-deps -e BASE_URL=https://example.test setlist-stash)
for _ in $(seq 1 30); do
  [ "$(docker inspect -f '{{.State.Running}}' "${cid}")" = "false" ] && break
  sleep 1
done
running=$(docker inspect -f '{{.State.Running}} {{.State.ExitCode}}' "${cid}")
docker logs "${cid}" > "${WORK}/https_refusal.log" 2>&1 || true
docker rm -f "${cid}" > /dev/null 2>&1 || true
[ "${running%% *}" = "false" ] \
  || fail "the app booted with the public session key on an https BASE_URL"
[ "${running##* }" != "0" ] || fail "https boot with the public key exited 0"
grep -q "SESSION_SECRET is still the shipped development default" \
  "${WORK}/https_refusal.log" || fail "https boot did not fail on SESSION_SECRET"
pass "app refuses the public session key on an https BASE_URL"

echo "SMOKE PASS in $(( $(date +%s) - start ))s (healthy after ${ready}s)"

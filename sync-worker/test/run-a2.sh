#!/usr/bin/env bash
#
# Local A2 harness: start `wrangler dev` (local/miniflare), run the A2 tests against
# it, then tear down. NEVER touches production. Run from anywhere:
#   bash sync-worker/test/run-a2.sh
#
set -uo pipefail
cd "$(dirname "$0")/.."   # -> sync-worker/

PORT="${A2_PORT:-8787}"
BASE="http://127.0.0.1:${PORT}"

# First transmitter code from the local .dev.vars, digit-normalized to match the worker's
# X-Director-Code handling (which strips non-digits). Never printed.
CODES=$(grep -E '^TRANSMITTER_CODES=' .dev.vars 2>/dev/null | head -1 | cut -d= -f2-)
CODE=$(printf '%s' "${CODES:-}" | cut -d, -f1 | tr -cd '0-9')
if [ -z "${CODE:-}" ]; then
  echo "✗ no usable digit TRANSMITTER_CODES in sync-worker/.dev.vars — cannot authorize test publishes"
  exit 1
fi

echo "→ starting wrangler dev on :${PORT} (local)…"
npx wrangler dev --port "${PORT}" >/tmp/sv-a2-wrangler.log 2>&1 &
WPID=$!
cleanup() { kill "${WPID}" 2>/dev/null; wait "${WPID}" 2>/dev/null; }
trap cleanup EXIT INT TERM

# Wait for the relay to answer /health (up to ~60s — first run compiles + boots miniflare).
up=0
for _ in $(seq 1 60); do
  if curl -sf "${BASE}/health" >/dev/null 2>&1; then up=1; break; fi
  sleep 1
done
if [ "${up}" != "1" ]; then
  echo "✗ wrangler dev never came up. Last log lines:"
  tail -25 /tmp/sv-a2-wrangler.log
  exit 1
fi
echo "→ relay up at ${BASE}; running A2 tests…"

RELAY_TEST_BASE="${BASE}" RELAY_TEST_CODE="${CODE}" node --test test/a2.test.mjs
RC=$?
echo "→ A2 harness exit code: ${RC}"
exit "${RC}"

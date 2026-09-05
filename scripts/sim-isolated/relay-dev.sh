#!/usr/bin/env bash
# relay-dev.sh — local relay for the ISOLATED simulator build: the sim app's RELAY_BASE is http://localhost:8787.
# --local keeps Durable Objects in-process; nothing here touches Cloudflare or the production room.
# Pass --fresh to drop persisted local state first (a stale page in the room would confuse a boot test).
set -euo pipefail
COPY="${SV_SIM_COPY:-$HOME/sv-sim-build}"
cd "$COPY/sync-worker"
if [ "${1:-}" = "--fresh" ]; then rm -rf .wrangler/state; fi
exec npx wrangler dev --local --port 8787 --ip 127.0.0.1

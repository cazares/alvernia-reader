#!/usr/bin/env bash
# pull-device-trace — one command: collect a device's own log, convert it, and score it.
#
# WHY A SCRIPT. Reading a device's unified log needs sudo, the `log` binary must be called by
# ABSOLUTE PATH (zsh has a `log` builtin that swallows the flags and fails with "too many
# arguments"), and the analyzers want JSONL. Three steps, each with a foot-gun, run while tired at
# the end of a long day. This is the whole loop.
#
# Needs build 438+ for the app's own breadcrumbs, and 442+ for them to PERSIST — 438-441 logged at
# .info, which os_log keeps in memory only and never writes to the store a collect reads.
#
# Usage:  sudo bash scripts/pull-device-trace.sh <UDID> [minutes]
#   mPad      00008030-000314A91443C02E
#   iPhone 17 00008150-0008299C0EBB401C
set -uo pipefail
UDID="${1:?usage: sudo bash scripts/pull-device-trace.sh <UDID> [minutes]}"
MINS="${2:-15}"
OUT="${HOME}/Desktop/sv-trace-$(date +%H%M%S)"
HERE="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> collecting last ${MINS}m from ${UDID}"
/usr/bin/log collect --device-udid "$UDID" --last "${MINS}m" --output "${OUT}.logarchive" || {
  echo "✖ collect failed. Is the device plugged in and unlocked? Did you run this with sudo?" >&2
  exit 2
}

echo "==> converting"
node "${HERE}/scripts/logarchive-to-jsonl.mjs" "${OUT}.logarchive" > "${OUT}.jsonl" || exit 2

# The archive and JSONL land in the invoking user's Desktop, not root's, and must be readable
# afterwards — sudo would otherwise leave root-owned files the analyzers cannot open.
if [ -n "${SUDO_USER:-}" ]; then chown -R "${SUDO_USER}" "${OUT}.logarchive" "${OUT}.jsonl" 2>/dev/null || true; fi

echo "==> rows: $(wc -l < "${OUT}.jsonl")"
echo
echo "############ JOIN LATENCY ############"
node "${HERE}/scripts/analyze-join-latency.mjs" "${OUT}.jsonl" || true
echo
echo "############ RESYNC / WEDGE ############"
node "${HERE}/scripts/analyze-resync.mjs" "${OUT}.jsonl" || true
echo
echo "trace kept at ${OUT}.jsonl"

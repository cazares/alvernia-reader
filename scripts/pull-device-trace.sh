#!/usr/bin/env bash
# pull-device-trace — one command: collect device logs, convert them, and score them.
#
# WHY A SCRIPT. Reading a device's unified log needs sudo, the `log` binary must be called by
# ABSOLUTE PATH (zsh has a `log` builtin that swallows the flags and fails with "too many
# arguments"), and the analyzers want JSONL. Three steps, each with a foot-gun, run while tired at
# the end of a long day. This is the whole loop.
#
# Needs build 438+ for the app's own breadcrumbs, and 442+ for them to PERSIST — 438-441 logged at
# .info, which os_log keeps in memory only and never writes to the store a collect reads.
#
# MULTIPLE DEVICES. Pass a comma-separated list to collect several in one run. The director-
# promotion bug family is only ever settled by looking at every device AT ONCE — a single device's
# log reads as an unexplained mystery ("why did this follower hunt for 20 minutes?") that a second
# device's log answers instantly ("because it was being hammered with rejected invites"). The
# per-device analyzers still run per device; analyze-promotion-capture runs on the POOLED set,
# which is the only way it can tell a ghost broadcast (a page every follower applied that no
# device logged sending) from an ordinary one.
#
# Collect the devices in the SAME run whenever you can: the pooled cross-device checks assume the
# clocks agree, and analyze-promotion-capture verifies that assumption from invite send/recv pairs
# rather than trusting it.
#
# Usage:  sudo bash scripts/pull-device-trace.sh <UDID>[,<UDID>...] [minutes]
#         bash scripts/pull-device-trace.sh --list          # every paired device + its real UDID
#   mPad        00008030-000314A91443C02E
#   iPhone 17   00008150-0008299C0EBB401C
#   Brau MASTER 91626b8e6d3bdd4fb336ad9199fea3edeebb9c93
#
# GETTING A UDID is a trap worth naming: the UUID that `xcrun devicectl list devices` PRINTS is a
# coredevice id, and `log collect` rejects it. The real hardware UDID is only in that command's
# --json-output, under hardwareProperties.udid — which is what `--list` below digs out. (Xcode >
# Window > Devices and Simulators shows the same value as "Identifier".)
set -uo pipefail
UDIDS="${1:?usage: sudo bash scripts/pull-device-trace.sh <UDID>[,<UDID>...] [minutes]  |  --list}"

if [ "$UDIDS" = "--list" ]; then
  TMP="$(mktemp -t svdev).json"
  xcrun devicectl list devices --json-output "$TMP" >/dev/null 2>&1 || {
    echo "✖ xcrun devicectl failed — is Xcode installed and selected?" >&2; exit 2; }
  node -e '
    const d = require(process.argv[1]);
    const rows = (d.result && d.result.devices) || [];
    if (!rows.length) { console.log("(no paired devices)"); process.exit(0); }
    for (const x of rows) {
      const name = (x.deviceProperties || {}).name || "?";
      const udid = (x.hardwareProperties || {}).udid || "(no udid)";
      const state = (x.connectionProperties || {}).tunnelState || "?";
      console.log(`${name.padEnd(14)} ${udid.padEnd(42)} ${state}`);
    }
    console.log("\nOnly a CONNECTED device can be collected. A device that was merely paired\nstill holds its own log — plug it in and it becomes collectable.");
  ' "$TMP"
  rm -f "$TMP"
  exit 0
fi
MINS="${2:-15}"
STAMP="$(date +%H%M%S)"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
JSONLS=()
FAILED=()

IFS=',' read -ra UDID_LIST <<< "$UDIDS"
for UDID in "${UDID_LIST[@]}"; do
  UDID="$(echo "$UDID" | tr -d '[:space:]')"
  [ -z "$UDID" ] && continue
  OUT="${HOME}/Desktop/sv-trace-${STAMP}-${UDID:0:8}"

  echo "==> collecting last ${MINS}m from ${UDID}"
  if ! /usr/bin/log collect --device-udid "$UDID" --last "${MINS}m" --output "${OUT}.logarchive"; then
    # One unreachable device must not throw away the devices that DID collect — a partial pool is
    # still evidence, and it says so in the summary rather than pretending it was complete.
    echo "✖ collect failed for ${UDID}. Plugged in, unlocked, trusted? Running under sudo?" >&2
    FAILED+=("$UDID")
    continue
  fi

  echo "==> converting ${UDID:0:8}"
  if ! node "${HERE}/scripts/logarchive-to-jsonl.mjs" "${OUT}.logarchive" > "${OUT}.jsonl"; then
    echo "✖ conversion produced nothing for ${UDID} (build < 438, or the app never ran)" >&2
    FAILED+=("$UDID")
    continue
  fi

  # The archive and JSONL land in the invoking user's Desktop, not root's, and must be readable
  # afterwards — sudo would otherwise leave root-owned files the analyzers cannot open.
  if [ -n "${SUDO_USER:-}" ]; then chown -R "${SUDO_USER}" "${OUT}.logarchive" "${OUT}.jsonl" 2>/dev/null || true; fi

  echo "==> rows: $(wc -l < "${OUT}.jsonl") (${OUT}.jsonl)"
  JSONLS+=("${OUT}.jsonl")
done

if [ ${#JSONLS[@]} -eq 0 ]; then
  echo "✖ nothing collected — no analysis possible." >&2
  exit 2
fi

for J in "${JSONLS[@]}"; do
  echo
  echo "############ JOIN LATENCY — $(basename "$J") ############"
  node "${HERE}/scripts/analyze-join-latency.mjs" "$J" || true
  echo
  echo "############ RESYNC / WEDGE — $(basename "$J") ############"
  node "${HERE}/scripts/analyze-resync.mjs" "$J" || true
done

echo
echo "############ PROMOTION CAPTURE (pooled, ${#JSONLS[@]} device(s)) ############"
node "${HERE}/scripts/analyze-promotion-capture.mjs" "${JSONLS[@]}" || true

echo
if [ ${#FAILED[@]} -gt 0 ]; then
  echo "⚠ ${#FAILED[@]} device(s) did NOT collect: ${FAILED[*]}"
  echo "  The pooled analysis above is PARTIAL. A ghost page can only be attributed to a sender"
  echo "  that was actually captured, so a missing device can make a legitimate broadcast look"
  echo "  like a ghost. Re-run with every device attached before trusting a FAIL."
fi
echo "traces kept at: ${JSONLS[*]}"

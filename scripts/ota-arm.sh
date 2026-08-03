#!/usr/bin/env bash
#
# ota-arm.sh — arm, roll back, or disarm the songbook OTA in one command.
#
# WHY --var AND NOT AN EDIT TO wrangler.jsonc:
#   e2e/bookUpdate.test.mjs:440 asserts BOOK_UPDATE_VERSION ships EMPTY. The repo must stay
#   dormant so a fresh clone can never arm anything by accident. `wrangler deploy --var`
#   overrides at deploy time only — git stays clean, and the abort path is a plain redeploy.
#
# WHY A BASE PER VERSION:
#   Cloudflare Pages keeps every deployment at its own immutable URL forever. Pointing
#   BOOK_UPDATE_BASE at a specific deployment gives each bookVersion a stable origin, which is
#   what makes ROLLBACK possible at all — signovivo.com only ever serves ONE manifest, so a
#   rollback target has to be fetched from its own deployment URL.
#
# THE "*" GUARD:
#   Fleet-wide arming needs BOTH "*" and --allow-fleet. `decideBookUpdate` throttles fleet starts
#   by returning null — which is BYTE-IDENTICAL to "disarmed", and the client treats an absent
#   pointer as a REVOKE and deletes ~27 MB of verified work. Named devices bypass the throttle
#   entirely, so prove it on ONE device first. Always.
#
# Usage:
#   scripts/ota-arm.sh --version bv_xxx --devices <id>[,<id>] [--base URL] [--skip-verify]
#   scripts/ota-arm.sh --disarm
#   scripts/ota-arm.sh --version bv_xxx --devices '*' --allow-fleet    # only after a device proves it
#
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION=""; DEVICES=""; BASE="https://signovivo.com"; ALLOW_FLEET=""; DISARM=0; SKIP_VERIFY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --devices) DEVICES="$2"; shift 2 ;;
    --base) BASE="${2%/}"; shift 2 ;;
    --allow-fleet) ALLOW_FLEET="yes"; shift ;;
    --disarm) DISARM=1; shift ;;
    --skip-verify) SKIP_VERIFY=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [ "$DISARM" = "1" ]; then
  echo "==> DISARM — clearing BOOK_UPDATE_VERSION (the ~20s abort)"
  echo "    Devices holding a STAGED copy delete it on their next check-in."
  echo "    Devices that already APPLIED keep the book they applied — disarm is not a rollback."
  cd sync-worker
  npx wrangler deploy --var BOOK_UPDATE_VERSION:"" --var BOOK_UPDATE_DEVICES:"" \
    --var BOOK_UPDATE_ALLOW_FLEET:"" 2>&1 | tail -6
  echo "✅ disarmed."
  exit 0
fi

[ -n "$VERSION" ] || { echo "✖ --version is required (bv_ + 16 hex)" >&2; exit 2; }
[ -n "$DEVICES" ] || { echo "✖ --devices is required (a device id, or '*' with --allow-fleet)" >&2; exit 2; }

# Malformed config fails CLOSED in the worker (bookArming.js:43); catch it here with a clearer error.
echo "$VERSION" | grep -qE '^bv_[0-9a-f]{16}$' || {
  echo "✖ --version must match bv_[0-9a-f]{16} — got '$VERSION'" >&2; exit 2; }

if [ "$DEVICES" = "*" ] && [ "$ALLOW_FLEET" != "yes" ]; then
  echo "✖ '*' without --allow-fleet does nothing (both switches are required by design)." >&2
  echo "  Prove it on ONE named device first — named devices bypass the fleet throttle." >&2
  exit 2
fi

echo "── OTA ARM ───────────────────────────────────────────────────────────────────"
echo "  bookVersion : $VERSION"
echo "  base        : $BASE"
echo "  devices     : $DEVICES"
[ "$ALLOW_FLEET" = "yes" ] && echo "  ⚠️  FLEET-WIDE — every device, throttled ${BOOK_UPDATE_CONCURRENCY:-2} at a time"
echo

# PRE-ARM GATE. Arming a version the base cannot serve byte-exact means the device pulls ~27 MB,
# fails verifyStaged, and shows on the dashboard exactly like a device that was never armed.
if [ "$SKIP_VERIFY" = "0" ]; then
  echo "==> Pre-arm: verifying $BASE serves $VERSION byte-exact"
  node scripts/verify-ota-fetchability.mjs --all --base "$BASE" | tail -4
  SERVED=$(curl -sS "$BASE/bundle-manifest.json?cb=$$" -H 'cache-control: no-cache' \
    | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{try{process.stdout.write(JSON.parse(s).bookVersion)}catch(e){process.stdout.write('PARSE_FAIL')}})")
  if [ "$SERVED" != "$VERSION" ]; then
    echo "✖ ABORT — $BASE serves '$SERVED', not '$VERSION'. Not arming." >&2
    exit 1
  fi
  echo "   ✅ $BASE serves exactly $VERSION"
  echo
fi

echo "==> Deploying worker with the arming vars (repo stays dormant)"
cd sync-worker
npx wrangler deploy \
  --var BOOK_UPDATE_VERSION:"$VERSION" \
  --var BOOK_UPDATE_DEVICES:"$DEVICES" \
  --var BOOK_UPDATE_ALLOW_FLEET:"$ALLOW_FLEET" \
  --var BOOK_UPDATE_BASE:"$BASE" 2>&1 | tail -6

cat <<EOF

✅ ARMED — $VERSION from $BASE, for: $DEVICES

Next:
  1. Foreground the app on that device (check-in is what delivers the pointer).
  2. Watch its Libro cell: downloading:NN% -> ready -> active.
  3. A HUMAN opens a named song and confirms it OUT LOUD. No hash catches a
     wrong-but-well-formed book; this is the only check independent of the build.

Abort:  scripts/ota-arm.sh --disarm
EOF

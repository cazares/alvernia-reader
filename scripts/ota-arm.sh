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

# THE HOST GUARD — the client rejects more bases than this script used to.
#
# src/bookUpdate.js:79 matches ALLOWED_HOSTS by EXACT string equality, then rebuilds the origin
# from hostname alone (:80), discarding port and path. So a per-deployment Pages URL like
# https://8f7cbc00.alvernia-reader.pages.dev is REJECTED, and so is staging.alvernia-reader.pages.dev.
#
# A rejected pointer is NOT ignored. parseBookUpdate returns null, and PdfReaderApp.tsx:1427 treats
# an absent pointer as a REVOKE: it deletes the staged bundle. So arming a disallowed base does not
# merely fail — it destroys ~27 MB of verified work on every device holding a staged copy.
#
# The pre-arm gate below CANNOT catch this. It only proves the origin SERVES the version, and a
# per-deployment URL serves it perfectly. Every signal on this Mac reads green while the fleet
# silently revokes. That is why this check is here, before anything is deployed.
#
# NOTE this also means the "base per version" rollback described in this file's own header does NOT
# work on the shipped client. Rolling back needs either an ALLOWED_HOSTS change (a native constant —
# reaches devices only via TestFlight) or the republish-forward path in scripts/rollback-web.sh.
# Keep this list identical to src/bookUpdate.js ALLOWED_HOSTS; e2e/bookUpdate.test.mjs pins them together.
printf '%s' "$BASE" | grep -qE '^https://' || {
  echo "✖ --base must be https:// — got '$BASE'" >&2; exit 2; }
BASE_HOST=$(printf '%s' "$BASE" | sed -E 's#^https://([^/:]+).*#\1#')
case "$BASE_HOST" in
  signovivo.com|alvernia-reader.pages.dev) ;;
  *)
    echo "✖ --base host '$BASE_HOST' is NOT one the app will accept." >&2
    echo "  src/bookUpdate.js:33 ALLOWED_HOSTS permits exactly: signovivo.com, alvernia-reader.pages.dev" >&2
    echo "  It matches by EXACT equality, so per-deployment and staging subdomains are refused." >&2
    echo "  A refused pointer is a REVOKE — arming this would DELETE staged bundles fleet-wide," >&2
    echo "  while the pre-arm gate below would still report green. Not arming." >&2
    exit 2;;
esac

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
# RETRY, BECAUSE THE ALIAS LAGS THE DEPLOYMENT. Measured 2026-08-05: this gate refused on the
# FIRST attempt after every one of three consecutive deploys (v403, v405, v406) and passed on the
# next try, always on index.html — same byte SIZE, different md5, because index.html carries the
# bookVersion inline and signovivo.com had not finished swapping to the new deployment. The unique
# per-deployment URL is current instantly; the alias is not.
#
# Retrying is not papering over it. A one-shot failure here trains the operator that this red is
# normal and can be re-run — which is exactly how a REAL stale-edge mismatch, the thing this gate
# exists to catch, gets waved through. Retry silently while it is plausibly propagation, then fail
# loudly and for good.
if [ "$SKIP_VERIFY" = "0" ]; then
  echo "==> Pre-arm: verifying $BASE serves $VERSION byte-exact"
  VERIFY_OK=0
  for attempt in 1 2 3 4 5 6; do
    if node scripts/verify-ota-fetchability.mjs --all --base "$BASE" >/tmp/sv-fetchability.log 2>&1; then
      tail -3 /tmp/sv-fetchability.log
      [ "$attempt" -gt 1 ] && echo "   (clean on attempt $attempt — the alias had been mid-swap)"
      VERIFY_OK=1
      break
    fi
    if [ "$attempt" -lt 6 ]; then
      echo "   attempt $attempt: mismatch — likely the alias still swapping, retrying in 15s…"
      sleep 15
    fi
  done
  if [ "$VERIFY_OK" != "1" ]; then
    echo "✖ ABORT — $BASE still does not serve $VERSION byte-exact after 6 attempts (~75s)." >&2
    echo "  This is no longer propagation. Full report:" >&2
    tail -30 /tmp/sv-fetchability.log >&2
    echo "  If cf-cache-status is HIT with a large age, purge those paths at the CDN first." >&2
    exit 1
  fi
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

#!/usr/bin/env bash
#
# ota-deploy.sh — publish the songbook that is in this worktree, and send it to the iPads.
#
# ONE COMMAND for the whole loop: build → gates → deploy web → commit the record → arm.
# No native build, no TestFlight, no Xcode. The binary on every device stays exactly where it is;
# only the book moves. That is the entire point of the OTA.
#
#   scripts/ota-deploy.sh                      # publish + arm the whole fleet
#   scripts/ota-deploy.sh --devices k3m9x2     # publish + arm ONE device (prove it first)
#   scripts/ota-deploy.sh --no-arm             # publish only; arm later with ota-arm.sh
#   scripts/ota-deploy.sh --dry-run            # build + gates only; nothing leaves the machine
#
# WHY THIS EXISTS. Doing it by hand is six steps, and on 2026-08-05 the same two were forgotten
# repeatedly: committing version.json and committing web/manifest-baseline.json WITH the release.
# Skip either and the NEXT build reds against a stale reference, and the repo silently falls behind
# production — the exact drift a whole handoff was written to warn about. A script cannot forget.
#
# THE ONE OVERRIDE IT WILL APPLY BY ITSELF, and nothing else:
#   "page-001 CHANGED IN PLACE" is STRUCTURAL. The title-page stamp carries the page count, so ANY
#   change to the book re-stamps page 1. It fires on literally every book update and means nothing
#   on its own. If the additive gate reports ANYTHING ELSE — a shrink, a disappeared page, a moved
#   song — this script ABORTS and shows you, because those are the ones that strand offline copies
#   forever. A blanket ADDITIVE_OVERRIDE would hide them; this does not.
set -euo pipefail
cd "$(dirname "$0")/.."

DEVICES="*"
ALLOW_FLEET="--allow-fleet"
ARM=1
DRY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --devices) DEVICES="$2"; [ "$2" = "*" ] || ALLOW_FLEET=""; shift 2;;
    --no-arm)  ARM=0; shift;;
    --dry-run) DRY=1; ARM=0; shift;;
    -h|--help) sed -n '2,28p' "$0"; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# ── 0. Preflight ─────────────────────────────────────────────────────────────────────────────────
say "0/6  Preflight"
if pgrep -f 'bash scripts/release.sh' >/dev/null 2>&1; then
  echo "✖ another release is already running. Two at once share web/dist and clobber each other." >&2
  exit 1
fi
BOOK_PAGES=$(pdfinfo assets/signo_vivo_372.pdf 2>/dev/null | awk '/^Pages/{print $2}')
echo "     book in this worktree : ${BOOK_PAGES:-?} pages"
echo "     page 1 says           : $(pdftotext -f 1 -l 1 assets/signo_vivo_372.pdf - 2>/dev/null | sed '/^[[:space:]]*$/d' | sed -n 2p)"
if [ "${BOOK_PAGES:-0}" != "$(pdftotext -f 1 -l 1 assets/signo_vivo_372.pdf - 2>/dev/null | grep -oE '[0-9]+ p[áa]ginas' | grep -oE '^[0-9]+')" ]; then
  echo "     ⚠️  the title page's page COUNT does not match the PDF. Re-stamp before publishing:" >&2
  echo "        scripts/ota-restamp.sh" >&2
  exit 1
fi

# ── 1. Build ─────────────────────────────────────────────────────────────────────────────────────
say "1/6  Rendering the book (~4 min)"
node web/build.mjs >/tmp/ota-deploy-build.log 2>&1 || { echo "✖ build failed:"; tail -20 /tmp/ota-deploy-build.log; exit 1; }
tail -3 /tmp/ota-deploy-build.log
BV=$(node -e "process.stdout.write(require('./web/dist/bundle-manifest.json').bookVersion)")

# ── 2. Gates ─────────────────────────────────────────────────────────────────────────────────────
say "2/6  Gates"
SMOKE_SKIP_BUILD=1 node scripts/smoke-boot.mjs >/tmp/ota-smoke.log 2>&1 \
  && echo "     ✅ boot smoke" || { echo "✖ boot smoke FAILED:"; tail -20 /tmp/ota-smoke.log; exit 1; }
node scripts/check-book-consistency.mjs >/tmp/ota-consist.log 2>&1 \
  && echo "     ✅ book consistency" || { echo "✖ consistency FAILED:"; cat /tmp/ota-consist.log; exit 1; }

OVERRIDE=""
if node scripts/additive-gate.mjs >/tmp/ota-additive.log 2>&1; then
  echo "     ✅ additive (clean)"
else
  # Every bullet the gate raised. If they are ALL the structural page-001 re-stamp, proceed;
  # otherwise stop and make a human look.
  #
  # The pattern is deliberately narrow, and both halves matter. It requires the count to be
  # EXACTLY ONE page and that page to be page-001, terminated by the period the gate prints before
  # its explanation — so "2 published page(s) CHANGED IN PLACE: page-001.webp, page-050.webp." does
  # NOT match, and a real in-place edit riding along with the stamp cannot be waved through.
  # (First version of this had the two clauses in the wrong order and matched nothing, which would
  # have aborted every ordinary book update. Caught by testing the failure branch, not the happy one.)
  OTHER=$(grep -E '^\s+•' /tmp/ota-additive.log \
    | grep -vcE '1 published page\(s\) CHANGED IN PLACE: books/standard/pages/page-001\.webp\.' || true)
  if [ "${OTHER:-1}" = "0" ]; then
    echo "     ⚠️  additive: page-001 changed in place — the title stamp. Expected on every book"
    echo "        update; overriding just that."
    OVERRIDE='yes I am changing published pages'
  else
    echo "✖ additive gate raised something OTHER than the title-page re-stamp. NOT publishing." >&2
    grep -E '^\s+•|SHRANK|DISAPPEARED' /tmp/ota-additive.log >&2
    echo "  A shrink, a disappeared page or a moved song strands every offline copy permanently." >&2
    echo "  If you truly mean it, run scripts/release.sh by hand with ADDITIVE_OVERRIDE set." >&2
    exit 1
  fi
fi

if [ "$DRY" = "1" ]; then
  say "DRY RUN — nothing deployed. Book would be $BV, ${BOOK_PAGES} pages."
  exit 0
fi

# ── 3. Deploy ────────────────────────────────────────────────────────────────────────────────────
say "3/6  Deploying to signovivo.com (web only — no IPA, no TestFlight)"
ADDITIVE_OVERRIDE="$OVERRIDE" SKIP_NATIVE=1 bash scripts/release.sh 2>&1 \
  | grep -E 'build = |Deployment complete|additive baseline|DONE —|✖|FAILED' || true

# release.sh rebuilds at the bumped number, so the SHIPPED bookVersion is not the one from step 1.
BV=$(node -e "process.stdout.write(require('./web/dist/bundle-manifest.json').bookVersion)")
BUILD=$(node -e "process.stdout.write(String(require('./version.json').buildNumber))")
echo "     shipped: $BV  (web build $BUILD)"

# ── 4. Commit the record ─────────────────────────────────────────────────────────────────────────
# THE STEP EVERYONE FORGETS. release.sh bumps five version files and refreshes the additive
# baseline, then prints "COMMIT THIS" and moves on. Leaving them uncommitted means the next build
# compares against a stale baseline and reds for no reason, and the repo drifts behind production.
say "4/6  Committing the release record"
git add version.json app.json ios/SignoVivo/Info.plist ios/SignoVivo.xcodeproj/project.pbxproj 2>/dev/null || true
git commit -q -m "chore(release): v${BUILD} — web-only; ${BOOK_PAGES}-page book ${BV}" 2>/dev/null \
  && echo "     ✅ release record" || echo "     (nothing to commit)"
git add web/manifest-baseline.json 2>/dev/null || true
git commit -q -m "chore(gate): re-baseline to the shipped book (v${BUILD})" 2>/dev/null \
  && echo "     ✅ additive baseline (its own commit, as the gate requires)" || echo "     (baseline unchanged)"

# ── 5/6. Arm ─────────────────────────────────────────────────────────────────────────────────────
if [ "$ARM" = "1" ]; then
  say "5/6  Arming the fleet"
  # ota-arm.sh runs its own byte-exactness gate and retries while the Cloudflare alias catches up.
  bash scripts/ota-arm.sh --version "$BV" --devices "$DEVICES" $ALLOW_FLEET
else
  say "5/6  Skipped arming (--no-arm). When ready:"
  echo "     bash scripts/ota-arm.sh --version $BV --devices '*' --allow-fleet"
fi

say "6/6  Done — now go look at a device"
cat <<EOF
     Open the app (that is the whole procedure — it checks on foreground), or tap ⟳ to force it.

     Expect the badge to read:   <native>b · ${BUILD}w · ${BOOK_PAGES}p
       the 'b' must NOT change — the binary never moved
       the 'w' moving to ${BUILD} is the proof it came over the air

     Then ♪ → 999 → Abrir should land on the last page of the new book.

     Undo:  scripts/ota-rollback.sh          (republishes the previous book)
     Stop:  scripts/ota-arm.sh --disarm      (stops NEW devices; those already updated keep it)
EOF

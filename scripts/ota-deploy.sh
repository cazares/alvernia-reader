#!/usr/bin/env bash
#
# ota-deploy.sh — publish the songbook that is in this worktree, and send it to the iPads.
#
# ONE COMMAND for the whole loop: build → deploy web → commit the record → arm.
# No native build, no TestFlight, no Xcode. The binary on every device stays exactly where it is;
# only the book moves. That is the entire point of the OTA.
#
#   scripts/ota-deploy.sh                      # publish + arm the whole fleet
#   scripts/ota-deploy.sh --devices k3m9x2     # publish + arm ONE device (prove it first)
#   scripts/ota-deploy.sh --no-arm             # publish only; arm later with ota-arm.sh
#   scripts/ota-deploy.sh --dry-run            # build only; nothing leaves the machine
#
# WHY THIS EXISTS. Doing it by hand is six steps, and on 2026-08-05 the same two were forgotten
# repeatedly: committing version.json and committing web/manifest-baseline.json WITH the release.
# Skip either and the NEXT build reds against a stale reference, and the repo silently falls behind
# production — the exact drift a whole handoff was written to warn about. A script cannot forget.
#
# IT DOES NOT JUDGE THE BOOK. The PDF you hand it is the book you meant to publish — any page
# count, any pages moved, any content. There are no gates here: no boot smoke, no book-consistency
# check, no additive gate. They ran on every publish and every one of them could only ever say no
# to the person who already decided. Undo is scripts/ota-rollback.sh.
#
#   Two consequences, stated once and never again. A shrink is the expensive one: a device offline
#   when the smaller book lands keeps what it has, and pages that no longer exist upstream cannot be
#   re-fetched. And a book that fails to render on a device now reaches it, because nothing here
#   opens the bundle first. Both are recoverable by republishing forward; neither is checked.
set -euo pipefail
cd "$(dirname "$0")/.."
export LANG=en_US.UTF-8

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
say "0/5  Preflight"
if pgrep -f 'bash scripts/release.sh' >/dev/null 2>&1; then
  echo "✖ another release is already running. Two at once share web/dist and clobber each other." >&2
  exit 1
fi
BOOK_PAGES=$(pdfinfo assets/songbook.pdf 2>/dev/null | awk '/^Pages/{print $2}')
[ -n "${BOOK_PAGES:-}" ] || { echo "✖ assets/songbook.pdf is not a readable PDF." >&2; exit 1; }
echo "     book in this worktree : ${BOOK_PAGES} pages"

# ── 1. Build ─────────────────────────────────────────────────────────────────────────────────────
say "1/5  Rendering the book (~4 min)"
node web/build.mjs >/tmp/ota-deploy-build.log 2>&1 || { echo "✖ build failed:"; tail -20 /tmp/ota-deploy-build.log; exit 1; }
tail -3 /tmp/ota-deploy-build.log
BV=$(node -e "process.stdout.write(require('./web/dist/bundle-manifest.json').bookVersion)")

if [ "$DRY" = "1" ]; then
  say "DRY RUN — nothing deployed. Book would be $BV, ${BOOK_PAGES} pages."
  exit 0
fi

# ── 2. Deploy ────────────────────────────────────────────────────────────────────────────────────
say "2/5  Deploying to signovivo.com (web only — no IPA, no TestFlight)"
# `... | grep … || true` swallowed release.sh's exit code entirely — `set -o pipefail` reports the
# pipeline's failure and `|| true` immediately discards it. A failed `wrangler pages deploy` (auth,
# network, quota) therefore printed its ✖, and this script sailed on to commit a release record for
# a build that never left the Mac and then ARM it. That was survivable while this script ran its own
# gates first; with the gates gone it is the only thing between a broken publish and the fleet.
#
# PIPESTATUS[0] is the deploy's real status, and it must be read on the VERY NEXT line: `|| true`
# would execute `true` and reset PIPESTATUS to (0), silently restoring the bug this replaces — which
# is exactly what the first version of this fix did, caught by testing the failure path rather than
# the happy one. `set +e` already stops a non-zero pipeline from aborting, so no `|| true` is needed
# and grep finding no lines is harmless.
set +e
SKIP_GATES=1 ADDITIVE_OVERRIDE='yes I am changing published pages' SKIP_NATIVE=1 bash scripts/release.sh 2>&1 \
  | grep -E 'build = |Deployment complete|additive baseline|DONE —|✖|FAILED'
RELEASE_RC=${PIPESTATUS[0]}
set -e
if [ "$RELEASE_RC" != "0" ]; then
  echo "" >&2
  echo "✖ release.sh exited $RELEASE_RC — NOTHING was published." >&2
  echo "  Not committing a release record and not arming: the fleet must never be pointed at a" >&2
  echo "  bookVersion that no origin serves. version.json may already be bumped; the next run" >&2
  echo "  reuses it harmlessly. Re-run once the cause above is fixed." >&2
  exit 1
fi

# release.sh rebuilds at the bumped number, so the SHIPPED bookVersion is not the one from step 1.
BV=$(node -e "process.stdout.write(require('./web/dist/bundle-manifest.json').bookVersion)")
BUILD=$(node -e "process.stdout.write(String(require('./version.json').buildNumber))")
echo "     shipped: $BV  (web build $BUILD)"

# ── 3. Commit the record ─────────────────────────────────────────────────────────────────────────
# THE STEP EVERYONE FORGETS. release.sh bumps five version files and refreshes the additive
# baseline, then prints "COMMIT THIS" and moves on. Leaving them uncommitted means the next build
# compares against a stale baseline and reds for no reason, and the repo drifts behind production.
say "3/5  Committing the release record"
# THE BOOK ITSELF. ota-publish.sh copies the new PDF over assets/songbook.pdf and nothing here ever
# committed it, which quietly broke the one thing the removal of every gate was justified BY:
#
#   - ota-rollback.sh refuses to run while assets/songbook.pdf is dirty ("commit or discard them
#     first"), so the advertised undo failed after EVERY publish — exactly when it is needed.
#   - ota-rollback.sh --list builds its history from `git log --follow -- assets/songbook.pdf`, so
#     the book just published was absent: --list labelled the PREVIOUS book "→ current", and the
#     default rollback target was two books back rather than one.
#
# Committing it here makes git the ledger it was always claimed to be. Deliberately in the SAME
# commit as version.json: the book and the build number that shipped it must never be separable.
git add assets/songbook.pdf 2>/dev/null || true
git add version.json app.json ios/SignoVivo/Info.plist ios/SignoVivo.xcodeproj/project.pbxproj 2>/dev/null || true
git commit -q -m "chore(release): v${BUILD} — web-only; ${BOOK_PAGES}-page book ${BV}" 2>/dev/null \
  && echo "     ✅ release record" || echo "     (nothing to commit)"
git add web/manifest-baseline.json 2>/dev/null || true
git commit -q -m "chore(gate): re-baseline to the shipped book (v${BUILD})" 2>/dev/null \
  && echo "     ✅ additive baseline (its own commit, as the gate requires)" || echo "     (baseline unchanged)"

# ── 4/5. Arm ─────────────────────────────────────────────────────────────────────────────────────
if [ "$ARM" = "1" ]; then
  say "4/5  Arming the fleet"
  # ota-arm.sh runs its own byte-exactness gate and retries while the Cloudflare alias catches up.
  bash scripts/ota-arm.sh --version "$BV" --devices "$DEVICES" $ALLOW_FLEET
else
  say "4/5  Skipped arming (--no-arm). When ready:"
  echo "     bash scripts/ota-arm.sh --version $BV --devices '*' --allow-fleet"
fi

say "5/5  Done — now go look at a device"
cat <<EOF
     Open the app (that is the whole procedure — it checks on foreground), or tap ⟳ to force it.

     Expect the badge to read:   <native>b · ${BUILD}w · ${BOOK_PAGES}p
       the 'b' must NOT change — the binary never moved
       the 'w' moving to ${BUILD} is the proof it came over the air

     Then ♪ → 999 → Abrir should land on the last page of the new book.

     Undo:  scripts/ota-rollback.sh          (republishes the previous book)
     Stop:  scripts/ota-arm.sh --disarm      (stops NEW devices; those already updated keep it)
EOF

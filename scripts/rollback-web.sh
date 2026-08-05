#!/usr/bin/env bash
#
# rollback-web.sh — the one-glance "undo the last web deploy" helper.
#
# Release Safety System (M1). When a deploy to signovivo.com goes wrong, this shows
# the recent Pages deployments and prints the exact steps to revert — so mid-Mass you
# copy one line instead of remembering the procedure. It is READ-ONLY: it never
# deploys, deletes, or changes anything. You run the printed step yourself.
#
# Cloudflare Pages keeps every deployment, so rollback is instant and does not need a
# rebuild. wrangler 4.x has no `pages rollback` subcommand, so there are three paths:
#
#   C) REPUBLISH FORWARD (the one to use when native devices are on the OTA): put the
#      old CONTENT back and publish it at a HIGHER build number. Restoring an old
#      deployment restores its old builtFromShellBuild, and decideBundle rule 7 then
#      makes every native device ignore the rollback while signovivo.com obeys it.
#   A) DASHBOARD (instant, WEB ONLY): find the last-good deployment, "Rollback to this".
#   B) CLI (rebuild a known-good commit, WEB ONLY, bypasses the publish gates).
#
# Usage:  bash scripts/rollback-web.sh
#
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT="alvernia-reader"
PROD_BRANCH="main"

echo "── SignoVivo web rollback helper (read-only) ─────────────────────────────────"
echo

# Confirm wrangler is authenticated (read-only). If not, say so plainly.
if ! npx wrangler whoami >/tmp/sv-whoami.log 2>&1; then
  echo "⚠  wrangler is not authenticated (or offline). Run 'npx wrangler login' first."
  echo "   You can still roll back from the Cloudflare dashboard (path A below)."
  echo
else
  echo "✓ wrangler authenticated as: $(grep -iE 'associated with the email|account' /tmp/sv-whoami.log | head -1 | sed 's/^[[:space:]]*//')"
  echo
  echo "Recent production deployments (newest first) — the top LIVE one is current:"
  echo "────────────────────────────────────────────────────────────────────────────"
  # Read-only listing. Never mutates anything.
  npx wrangler pages deployment list --project-name "$PROJECT" 2>/tmp/sv-deploylist.log \
    || { echo "  (could not list deployments — see /tmp/sv-deploylist.log; use path A below)"; }
  echo "────────────────────────────────────────────────────────────────────────────"
  echo
fi

cat <<EOF
⚠  READ THIS FIRST IF ANY NATIVE DEVICE IS ON THE OTA.

   A rollback that RESTORES AN OLD DEPLOYMENT (path A or B) also restores that book's
   OLD builtFromShellBuild. decideBundle rule 7 (src/bookResolve.js) then compares it
   against the build baked into the app and refuses anything at or below it — so the
   iPads keep booting their BAKED book and silently ignore the rollback, while
   signovivo.com correctly shows the old one. Web and native disagree, quietly.

   For a fleet on the OTA, use path C. It publishes the SAME OLD CONTENT FORWARD at a
   HIGHER build number, so rule 7 is never tripped and every device takes it.
   A rollback here is a forward deploy of older bytes.

   Paths A and B remain correct when only signovivo.com matters (no native fleet, or
   the fleet is deliberately staying put).

HOW TO ROLL BACK (pick one):

  C) REPUBLISH FORWARD — the one to use when native devices are on the OTA:
       1. Put the OLD content back in the working tree (git checkout <GOOD_SHA> --
          assets/ web/src/  — content only, NOT version.json).
       2. node web/build.mjs
       3. SKIP_NATIVE=1 bash scripts/release.sh
          (bumps version.json, so builtFromShellBuild goes UP while the book goes BACK,
           and every publish gate still runs — unlike path B)
       4. Re-arm at the NEW bookVersion printed by the release:
            bash scripts/ota-arm.sh --version bv_xxxxxxxx --devices '*' --allow-fleet
          The pointer must name the bookVersion prod is actually serving, or stageBook
          aborts with version-mismatch and nothing moves.

  A) DASHBOARD — instant, no rebuild.  WEB ONLY; see the warning above:
       1. https://dash.cloudflare.com  ->  Workers & Pages  ->  $PROJECT  ->  Deployments
       2. Find the last deployment that was GOOD (before the bad one).
       3. Click its "…" menu  ->  "Rollback to this deployment".
     Online followers pick up the reverted shell within ~60s (stale-while-revalidate).

  B) CLI — rebuild a known-good commit and redeploy to prod.  WEB ONLY, and it also
     BYPASSES every publish gate (boot smoke, additive, consistency, completeness)
     that release.sh runs. Prefer C:
       Recent commits on this branch:
$(git log --oneline -6 | sed 's/^/         /')

       # replace <GOOD_SHA> with the last-good commit, then:
       git stash --include-untracked           # park any uncommitted work
       git checkout <GOOD_SHA>
       node web/build.mjs
       npx wrangler pages deploy web/dist --project-name $PROJECT --branch $PROD_BRANCH --commit-dirty=true
       git checkout -                           # back to your branch
       git stash pop                            # restore parked work (if any)

  After rolling back, verify in a PRIVATE/incognito tab (the service worker serves
  the old shell "one load later" in a normal tab).

This script changed nothing. Run the step above to actually roll back.
EOF

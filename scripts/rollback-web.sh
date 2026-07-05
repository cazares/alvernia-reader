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
# rebuild. wrangler 4.x has no `pages rollback` subcommand, so there are two paths:
#
#   A) DASHBOARD (instant, recommended): open the project, find the last-good
#      deployment, and click its "…" -> "Rollback to this deployment".
#   B) CLI (rebuild a known-good commit and redeploy) — printed below.
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
HOW TO ROLL BACK (pick one):

  A) DASHBOARD — instant, most reliable, no rebuild:
       1. https://dash.cloudflare.com  ->  Workers & Pages  ->  $PROJECT  ->  Deployments
       2. Find the last deployment that was GOOD (before the bad one).
       3. Click its "…" menu  ->  "Rollback to this deployment".
     Online followers pick up the reverted shell within ~60s (stale-while-revalidate).

  B) CLI — rebuild a known-good commit and redeploy to prod:
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

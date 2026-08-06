#!/usr/bin/env bash
#
# ota-rollback.sh — put the PREVIOUS songbook back on the iPads.
#
#   scripts/ota-rollback.sh --list          # which books can I go back to?
#   scripts/ota-rollback.sh                 # go back one book
#   scripts/ota-rollback.sh --to <sha>      # go back to a specific one
#
# GIT IS THE LEDGER. A "rollback ledger" mapping bookVersion → Cloudflare deployment URL was
# proposed and is a WRITE-ONLY FILE — see below. It is also unnecessary: every published book is a
# commit of assets/*.pdf plus src/alverniaManual2SongIndex.js, so the history already records every
# book that ever shipped, in order, with its date and message. This script reads that.
#
# ROLLBACK IS REPUBLISH-FORWARD. There is no other kind, and this is not a shortcut:
#
#   The obvious mechanism — re-arm the OLD bookVersion with --base pointing at its immutable
#   Cloudflare deployment URL — CANNOT WORK on the shipped client. src/bookUpdate.js matches
#   ALLOWED_HOSTS by EXACT string equality, so <hash>.alvernia-reader.pages.dev is refused; and a
#   refused pointer is not ignored, it is the REVOKE path, which DELETES the staged bundle. You
#   would destroy ~27 MB on every device while every gate on this Mac reported success.
#   scripts/ota-arm.sh now blocks that base outright.
#
#   So: rebuild the old book, publish it as a NEW version, arm that. The CONTENT goes back; the
#   version numbers only ever climb. A device that rolls back from web 407 to the book that shipped
#   as 405 lands on web 408 carrying 405's content. That is expected, not a bug.
#
# THE SHRINK IS ALLOWED, which is what makes this possible at all: both the additive gate and the
# device's verifyStaged permit a book to lose trailing pages ABOVE its last song page. Going back
# from 374 songs to 373 is fine; going back to a book whose pages no longer cover its own songs is
# not, and both refuse it.
set -euo pipefail
cd "$(dirname "$0")/.."

BOOK=assets/songbook.pdf
INDEX=src/alverniaManual2SongIndex.js
TARGET=""
LIST=0
while [ $# -gt 0 ]; do
  case "$1" in
    --list) LIST=1; shift;;
    --to)   TARGET="$2"; shift 2;;
    -h|--help) sed -n '2,36p' "$0"; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# Every commit that changed the BOOK, newest first. That is the list of books that ever shipped.
#
# --follow is load-bearing: the book was renamed from signo_vivo_<pageCount>.pdf to a stable name,
# and without it `git log` stops dead at the rename and reports "fewer than two book versions",
# hiding every book that shipped before it. Every rollback target older than the rename would have
# been invisible.
#
# Built with a read loop rather than `mapfile`: macOS ships bash 3.2, which does not have it, and
# this script has to run on the machine that actually cuts releases.
HIST=()
while IFS= read -r line; do
  [ -n "$line" ] && HIST+=("$line")
done < <(git log --follow --format='%h|%ad|%s' --date=short -20 -- "$BOOK" 2>/dev/null)

if [ "${#HIST[@]}" -lt 2 ]; then
  echo "✖ fewer than two book versions in history — nothing to roll back to." >&2
  exit 1
fi

if [ "$LIST" = "1" ]; then
  say "Books in history (newest first) — 'current' is what is published now"
  i=0
  for h in "${HIST[@]}"; do
    sha=${h%%|*}; rest=${h#*|}; date=${rest%%|*}; subj=${rest#*|}
    pages=$(git show "$sha:$BOOK" 2>/dev/null | pdfinfo - 2>/dev/null | awk '/^Pages/{print $2}')
    mark="  "; [ "$i" = "0" ] && mark="→ "; [ "$i" = "1" ] && mark="⏪"
    printf "%s %s  %s  %4s pages  %s\n" "$mark" "$sha" "$date" "${pages:-?}" "$(echo "$subj" | cut -c1-52)"
    i=$((i+1))
  done
  echo
  echo "  → current   ⏪ what --to defaults to"
  echo "  scripts/ota-rollback.sh --to <sha>"
  exit 0
fi

# Default target: the book BEFORE the current one.
if [ -z "$TARGET" ]; then
  TARGET=$(echo "${HIST[1]}" | cut -d'|' -f1)
fi
TARGET_SUBJ=$(git log -1 --format='%s' "$TARGET" 2>/dev/null | cut -c1-60)
TARGET_PAGES=$(git show "$TARGET:$BOOK" 2>/dev/null | pdfinfo - 2>/dev/null | awk '/^Pages/{print $2}')
NOW_PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}')

say "Rolling back"
echo "     from : $NOW_PAGES pages (published now)"
echo "     to   : $TARGET_PAGES pages — $TARGET  $TARGET_SUBJ"
[ -n "$TARGET_PAGES" ] || { echo "✖ $TARGET has no $BOOK" >&2; exit 1; }

if [ -n "$(git status --porcelain "$BOOK" "$INDEX")" ]; then
  echo "✖ $BOOK / $INDEX have uncommitted changes. Commit or discard them first —" >&2
  echo "  a rollback must start from a known published book, not a half-edited one." >&2
  exit 1
fi

# ── Restore the old book + its song index ────────────────────────────────────────────────────────
git checkout "$TARGET" -- "$BOOK" "$INDEX"
echo "     restored $BOOK and $INDEX from $TARGET"

# ── Re-stamp page 1 ──────────────────────────────────────────────────────────────────────────────
# The restored page 1 carries the stamp it had when it shipped — an OLD date. Leaving it would put
# a book on the iPads whose title page claims a date it was not published on, which defeats the one
# offline staleness check the director has ("¿la suya dice agosto?"). stamp-book-date refuses to
# double-stamp, so page 1 is rebuilt from the unstamped original first, per its own documented
# recipe. UNSTAMPED_SRC is the pre-stamp book; it is only ever read.
UNSTAMPED_SRC=$(git rev-list --all --max-count=1 -- assets/signo_vivo_371.pdf 2>/dev/null || true)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
if [ -n "$UNSTAMPED_SRC" ] && git show "${UNSTAMPED_SRC}^:assets/signo_vivo_371.pdf" > "$TMP/unstamped.pdf" 2>/dev/null; then
  qpdf --empty --pages "$TMP/unstamped.pdf" 1 "$BOOK" 2-z -- "$TMP/clean.pdf"
  cp "$TMP/clean.pdf" "$BOOK"
  node scripts/stamp-book-date.mjs --pdf "$BOOK" | grep -E '✅|❌'
else
  echo "     ⚠️  could not find an unstamped page 1; leaving the restored stamp as-is." >&2
  echo "        The title page will show the date this book ORIGINALLY shipped." >&2
fi

say "Publishing it"
exec bash scripts/ota-deploy.sh "$@"

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

# WHERE THE BOOK LIVED AT A GIVEN COMMIT. It was renamed from signo_vivo_<pageCount>.pdf to a
# stable name, so "$sha:$BOOK" does not resolve for anything older than the rename — and every
# rollback target older than that is exactly what you reach for in an emergency. Resolve the real
# path per commit instead of assuming today's.
#
# The `|| true` matters as much as the lookup: under `set -e`, a command substitution that fails
# aborts the script, so the first pre-rename commit silently truncated the whole listing to one row
# rather than reporting anything.
book_path_at() {
  local sha="$1"
  if git cat-file -e "${sha}:${BOOK}" 2>/dev/null; then printf '%s' "$BOOK"; return; fi
  git ls-tree --name-only "$sha" assets/ 2>/dev/null | grep -iE '\.pdf$' | head -1 || true
}
pages_at() {
  local sha="$1" p
  p=$(book_path_at "$sha")
  [ -n "$p" ] || return 0
  git show "${sha}:${p}" 2>/dev/null | pdfinfo - 2>/dev/null | awk '/^Pages/{print $2}' || true
}

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
    pages=$(pages_at "$sha")
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
TARGET_PAGES=$(pages_at "$TARGET")
TARGET_PATH=$(book_path_at "$TARGET")
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
# Restore from the path the book HAD at that commit — pre-rename targets are not at $BOOK — and
# write it to today's name. git checkout cannot rename, so the PDF goes through git show.
git show "${TARGET}:${TARGET_PATH}" > "$BOOK"
git checkout "$TARGET" -- "$INDEX"
echo "     restored $BOOK (from ${TARGET_PATH}) and $INDEX @ $TARGET"

# The restored book goes back EXACTLY as it shipped, page 1 included. The version that lived here
# rebuilt page 1 and re-stamped it with today's date, by grafting a title page out of an old commit
# of assets/signo_vivo_371.pdf — a path the 2026-08-05 rename deleted, whose surviving revision was
# itself already stamped. So it welded a "371 páginas" cover onto whatever book you rolled back to.
# A rollback restores a book; it does not author a new one.

say "Publishing it"
exec bash scripts/ota-deploy.sh "$@"

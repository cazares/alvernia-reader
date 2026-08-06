#!/usr/bin/env bash
#
# ota-publish.sh — THE ONE COMMAND. Give it a new songbook PDF; it reaches the iPads.
#
#   scripts/ota-publish.sh ~/Downloads/nuevo.pdf
#
# That is the whole procedure. It installs the PDF, renders it, deploys to signovivo.com, commits
# the release record, and arms the fleet. No Xcode, no TestFlight, no App Store — the binary on
# every device stays exactly where it is and only the book moves.
#
# ANY PDF WORKS. Your PDF ships as your PDF: nothing grafts a page, deletes a page, draws on one,
# or refuses one. Song number = page number, derived from the PDF itself, so there is no index to
# edit and no page count to keep in sync anywhere.
#
#   scripts/ota-publish.sh <pdf> --devices k3m9x2   # prove it on ONE iPad before the fleet
#   scripts/ota-publish.sh <pdf> --dry-run          # render only; nothing leaves the Mac
#   scripts/ota-publish.sh --dry-run                # no pdf = check the book already in the repo
#
# Undo:  scripts/ota-rollback.sh          (puts the previous book back)
# Stop:  scripts/ota-arm.sh --disarm      (stops NEW devices; ones already updated keep it)
set -euo pipefail
cd "$(dirname "$0")/.."
export LANG=en_US.UTF-8

BOOK=assets/songbook.pdf
SRC=""
PASSTHRU=()
while [ $# -gt 0 ]; do
  case "$1" in
    --devices) PASSTHRU+=("$1" "$2"); shift 2;;
    --dry-run|--no-arm) PASSTHRU+=("$1"); shift;;
    -h|--help) awk 'NR>1 && /^#/ {sub(/^# ?/,""); print; next} NR>1 {exit}' "$0"; exit 0;;
    -*) echo "unknown arg: $1" >&2; exit 2;;
    *) SRC="$1"; shift;;
  esac
done

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# ── 1. Take the new book ─────────────────────────────────────────────────────────────────────────
if [ -n "$SRC" ]; then
  say "1/2  Installing $SRC"
  [ -f "$SRC" ] || { echo "✖ no such file: $SRC" >&2; exit 1; }
  NEW_PAGES=$(pdfinfo "$SRC" 2>/dev/null | awk '/^Pages/{print $2}') || true
  [ -n "${NEW_PAGES:-}" ] || { echo "✖ $SRC is not a readable PDF (pdfinfo could not open it)." >&2; exit 1; }
  OLD_PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}' || echo 0)
  echo "     $OLD_PAGES pages  ->  $NEW_PAGES pages"
  cp "$SRC" "$BOOK"
else
  say "1/2  No PDF given — publishing the book already in the repo"
  NEW_PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}')
  echo "     $NEW_PAGES pages"
fi

# ── 2. Everything else ───────────────────────────────────────────────────────────────────────────
# Two stages used to live here and both are gone, 2026-08-05, by the owner's call.
#
#   SONG INDEX. A hand-maintained [song, page] list that had to be edited for every new book. Song
#   number is now page number, derived from the PDF at build time (web/build.mjs). Nothing to edit.
#
#   TITLE-PAGE STAMP. Overlaid a date + page count on page 1 as an offline staleness check. It
#   rebuilt page 1 by grafting it from a hardcoded commit of a filename the 2026-08-05 rename had
#   deleted, and the revision it found was already stamped — so it welded a 371-page cover onto the
#   373-page book, wrote that over assets/songbook.pdf, and only then failed. Every publish went
#   through it. Want a date on the title page? Put it in whatever makes the PDF.
say "2/2  Publishing"
exec bash scripts/ota-deploy.sh ${PASSTHRU[@]+"${PASSTHRU[@]}"}

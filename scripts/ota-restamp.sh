#!/usr/bin/env bash
#
# ota-restamp.sh — refresh the title-page stamp so it tells the truth.
#
#   scripts/ota-restamp.sh                       # stamp with right now, Central
#   scripts/ota-restamp.sh --date 2026-08-05 --time 14:30
#
# WHY THIS IS NOT JUST "RUN stamp-book-date.mjs". That script OVERLAYS; it does not replace. Run it
# on an already-stamped book and the new line is painted on top of the old one — same corner, same
# font, same size — producing unreadable overlapping glyphs on the page the whole choir opens.
# Nothing downstream catches it: the page count is unchanged, the geometry is unchanged, every gate
# still passes. It renders as a smudge. So the script refuses, correctly, and tells you to rebuild
# page 1 from an unstamped source first.
#
# That rebuild is a two-command qpdf dance nobody remembers under pressure, and getting it wrong
# either smudges the title page or silently grafts a page 1 from the wrong edition. This does it.
#
# WHY IT MATTERS AT ALL. The stamp is the ONLY way to tell which songbook a device is holding with
# no internet — "¿la suya dice agosto?" across a room in five seconds. The build badge answers a
# different question (the shell's number) and can read "current" over a book months old. So a stamp
# that disagrees with the book is worse than no stamp: it is a check that lies.
#
# Found in the wild 2026-08-05: the title page read "· 372 páginas" on a 371-page book for a full
# day, because a release removed a page and never re-stamped.
set -euo pipefail
cd "$(dirname "$0")/.."
export LANG=en_US.UTF-8

BOOK=assets/songbook.pdf
PASSTHRU=()
while [ $# -gt 0 ]; do
  case "$1" in
    --date|--time) PASSTHRU+=("$1" "$2"); shift 2;;
    -h|--help) sed -n '2,26p' "$0"; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}')
BEFORE=$(pdftotext -f 1 -l 1 "$BOOK" - 2>/dev/null | sed '/^[[:space:]]*$/d' | sed -n 2p)
echo "  book   : ${PAGES:-?} pages"
echo "  before : ${BEFORE:-(unstamped)}"

# Find the last commit that still had the pre-stamp book, and take page 1 from it. Pages 2..N come
# from the CURRENT book, so only the title page is replaced — everything else stays byte-identical,
# which is what keeps the additive gate reporting a single page-001 change instead of a rewrite.
UNSTAMPED_COMMIT=$(git rev-list --all --max-count=1 -- assets/signo_vivo_371.pdf 2>/dev/null || true)
[ -n "$UNSTAMPED_COMMIT" ] || { echo "✖ cannot locate an unstamped source book in git history." >&2; exit 1; }

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
git show "${UNSTAMPED_COMMIT}^:assets/signo_vivo_371.pdf" > "$TMP/unstamped.pdf" 2>/dev/null \
  || { echo "✖ could not read the unstamped book from ${UNSTAMPED_COMMIT}^" >&2; exit 1; }

qpdf --empty --pages "$TMP/unstamped.pdf" 1 "$BOOK" 2-z -- "$TMP/clean.pdf"
CLEAN_PAGES=$(pdfinfo "$TMP/clean.pdf" | awk '/^Pages/{print $2}')
[ "$CLEAN_PAGES" = "$PAGES" ] || {
  echo "✖ rebuilt book has $CLEAN_PAGES pages, expected $PAGES. Refusing to write." >&2; exit 1; }

cp "$TMP/clean.pdf" "$BOOK"
node scripts/stamp-book-date.mjs --pdf "$BOOK" "${PASSTHRU[@]+"${PASSTHRU[@]}"}" | grep -E '✅|❌'

AFTER=$(pdftotext -f 1 -l 1 "$BOOK" - 2>/dev/null | sed '/^[[:space:]]*$/d' | sed -n 2p)
STAMPED_PAGES=$(printf '%s' "$AFTER" | grep -oE '[0-9]+' | tail -1)
echo "  after  : $AFTER"
[ "$STAMPED_PAGES" = "$PAGES" ] \
  && echo "  ✅ stamp matches the book ($PAGES pages) — safe to publish" \
  || { echo "  ✖ stamp says $STAMPED_PAGES but the book has $PAGES" >&2; exit 1; }
echo
echo "  Next:  scripts/ota-deploy.sh"

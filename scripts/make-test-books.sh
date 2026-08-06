#!/usr/bin/env bash
#
# make-test-books.sh — build songbook variants for exercising the OTA in every direction.
#
#   scripts/make-test-books.sh                    # 371, 372, 374 pages into ~/Desktop/ota-test
#   scripts/make-test-books.sh 370 375            # any counts you like
#   scripts/make-test-books.sh --out /tmp/books   # somewhere else
#
# WHY THIS IS A SCRIPT. The three books that first exercised deploy -> rollback -> deploy were made
# with ad-hoc qpdf lines in a shell that is now gone. Every finding survived; the instrument did not.
# Anyone repeating the test would have rebuilt it from scratch and guessed at the details that
# actually matter — which is exactly the "commit the script, not just the result" rule.
#
# THE DETAILS THAT MATTER, and why they are not arbitrary:
#
#   SHRINKS TAKE THE FIRST N PAGES. Only trailing pages may disappear. Both the additive gate and
#   the device's verifyStaged permit a book to lose pages ABOVE its last song, and that permission
#   is the entire reason republish-forward rollback works at all. A book cut from the middle tests
#   something the system is not designed to survive — worth doing deliberately one day, never by
#   accident while you are trying to prove the happy path.
#
#   GROWTH USES append-number-page.mjs. The appended page renders one huge number, readable across
#   a room. Jumping to an out-of-range song clamps to the last page, so the 999 trick tells you
#   which book a device is holding at a glance — no badge, no squinting, no internet.
#
#   NOTHING IS WRITTEN INTO THE REPO. These are disposable test artifacts, and a stray songbook in
#   assets/ is the exact shape of accident that publishes the wrong book.
set -euo pipefail
cd "$(dirname "$0")/.."
export LANG=en_US.UTF-8

BOOK=assets/songbook.pdf
OUT="$HOME/Desktop/ota-test"
COUNTS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --out) OUT="$2"; shift 2;;
    --book) BOOK="$2"; shift 2;;
    -h|--help) awk 'NR>1 && /^#/ {sub(/^# ?/,""); print; next} NR>1 {exit}' "$0"; exit 0;;
    -*) echo "unknown arg: $1" >&2; exit 2;;
    *) COUNTS+=("$1"); shift;;
  esac
done
[ "${#COUNTS[@]}" -gt 0 ] || COUNTS=(371 372 374)

[ -f "$BOOK" ] || { echo "no such book: $BOOK" >&2; exit 1; }
SRC_PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}')
[ -n "${SRC_PAGES:-}" ] || { echo "$BOOK is not a readable PDF." >&2; exit 1; }
mkdir -p "$OUT"
echo "source: $BOOK ($SRC_PAGES pages)  ->  $OUT"
echo

for N in "${COUNTS[@]}"; do
  DEST="$OUT/songbook-${N}p.pdf"
  if [ "$N" -lt "$SRC_PAGES" ]; then
    qpdf "$BOOK" --pages . 1-"$N" -- "$DEST"
    NOTE="shrink — trailing pages dropped"
  elif [ "$N" -gt "$SRC_PAGES" ]; then
    # One appended page per step. Beyond +1 this loops, so each added page carries its own number
    # rather than N-1 duplicates of the same one.
    cp "$BOOK" "$DEST"
    n=$SRC_PAGES
    while [ "$n" -lt "$N" ]; do
      n=$((n+1))
      node scripts/append-number-page.mjs --pdf "$DEST" --out "$DEST.tmp" --number "$n" >/dev/null
      mv -f "$DEST.tmp" "$DEST"
    done
    NOTE="grow — canary page(s) appended, each showing its own number"
  else
    cp "$BOOK" "$DEST"; NOTE="same size as the source"
  fi
  GOT=$(pdfinfo "$DEST" 2>/dev/null | awk '/^Pages/{print $2}')
  [ "$GOT" = "$N" ] || { echo "$DEST came out $GOT pages, expected $N" >&2; exit 1; }
  # Page 1 must survive intact: it is what a person looks at to identify the book, and a changed
  # page 1 turns every publish into a "page-001 CHANGED IN PLACE" report that means nothing.
  A=$(pdftotext -f 1 -l 1 "$BOOK" - 2>/dev/null | md5)
  B=$(pdftotext -f 1 -l 1 "$DEST" - 2>/dev/null | md5)
  [ "$A" = "$B" ] && P1="page 1 intact" || P1="PAGE 1 DIFFERS"
  printf '  OK  %-24s %3s pages  %s  (%s)\n' "$(basename "$DEST")" "$GOT" "$P1" "$NOTE"
done

cat <<EOF

Publish one:   scripts/ota-publish.sh $OUT/songbook-<N>p.pdf
Undo:          scripts/ota-rollback.sh
On a device:   the music note, then 999, then Abrir — the number on the last page is which book it holds.
EOF

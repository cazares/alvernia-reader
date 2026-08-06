#!/usr/bin/env bash
#
# ota-publish.sh — THE ONE COMMAND. Give it a new songbook PDF; it reaches the iPads.
#
#   scripts/ota-publish.sh ~/Downloads/nuevo.pdf
#
# That is the whole procedure. It installs the PDF, fixes the title-page stamp, renders the book,
# runs every gate, deploys to signovivo.com, commits the release record, and arms the fleet. No
# Xcode, no TestFlight, no App Store — the binary on every device stays exactly where it is and
# only the book moves.
#
#   scripts/ota-publish.sh <pdf> --devices k3m9x2   # prove it on ONE iPad before the fleet
#   scripts/ota-publish.sh <pdf> --dry-run          # stop after the gates; nothing leaves the Mac
#   scripts/ota-publish.sh --dry-run                # no pdf = check the book already in the repo
#
# Undo:  scripts/ota-rollback.sh          (puts the previous book back)
# Stop:  scripts/ota-arm.sh --disarm      (stops NEW devices; ones already updated keep it)
set -euo pipefail
cd "$(dirname "$0")/.."
export LANG=en_US.UTF-8

BOOK=assets/songbook.pdf
INDEX=src/alverniaManual2SongIndex.js
SRC=""
PASSTHRU=()
while [ $# -gt 0 ]; do
  case "$1" in
    --devices) PASSTHRU+=("$1" "$2"); shift 2;;
    --dry-run|--no-arm) PASSTHRU+=("$1"); shift;;
    -h|--help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    -*) echo "unknown arg: $1" >&2; exit 2;;
    *) SRC="$1"; shift;;
  esac
done

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# ── 1. Take the new book ─────────────────────────────────────────────────────────────────────────
if [ -n "$SRC" ]; then
  say "1/4  Installing $SRC"
  [ -f "$SRC" ] || { echo "✖ no such file: $SRC" >&2; exit 1; }
  NEW_PAGES=$(pdfinfo "$SRC" 2>/dev/null | awk '/^Pages/{print $2}') || true
  [ -n "${NEW_PAGES:-}" ] || { echo "✖ $SRC is not a readable PDF (pdfinfo could not open it)." >&2; exit 1; }
  OLD_PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}' || echo 0)
  echo "     $OLD_PAGES pages  ->  $NEW_PAGES pages"
  cp "$SRC" "$BOOK"
else
  say "1/4  No PDF given — publishing the book already in the repo"
  NEW_PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}')
  echo "     $NEW_PAGES pages"
fi

# ── 2. Keep the song index honest ────────────────────────────────────────────────────────────────
# A page is not a song. songIndex is a hand-maintained [song, page] list (web/build.mjs reads it),
# and check-book-consistency fails the build if a song points past the last page. When a book grows
# by pages APPENDED AT THE END, the new songs are [N, N] and can be added mechanically. Anything
# else — songs inserted in the middle, renumbered, removed — changes existing mappings and CANNOT
# be guessed; the gates will catch it and this script says so rather than inventing entries.
say "2/4  Song index"
IDX_MAX=$(node -e '
  const fs=require("fs");
  const m=[...fs.readFileSync(process.argv[1],"utf8").matchAll(/\[(\d+),\s*(\d+)\]/g)].map(x=>[+x[1],+x[2]]);
  process.stdout.write(String(Math.max(...m.map(a=>a[1]))));
' "$INDEX")
echo "     highest indexed page: $IDX_MAX   book: $NEW_PAGES pages"
if [ "$NEW_PAGES" -gt "$IDX_MAX" ]; then
  echo "     appending songs $((IDX_MAX+1))..$NEW_PAGES to the index"
  node -e '
    const fs=require("fs"); const [file,from,to]=process.argv.slice(1);
    let s=fs.readFileSync(file,"utf8");
    const add=[]; for(let n=+from;n<=+to;n++) add.push(`[${n}, ${n}]`);
    s=s.replace(/\n\];/, ", " + add.join(", ") + ",\n];");
    fs.writeFileSync(file,s);
  ' "$INDEX" "$((IDX_MAX+1))" "$NEW_PAGES"
  echo "     ✅ index now covers $NEW_PAGES"
elif [ "$NEW_PAGES" -lt "$IDX_MAX" ]; then
  echo "  ✖ the book has $NEW_PAGES pages but the song index points at page $IDX_MAX." >&2
  echo "    Songs were removed or renumbered — that is not something this script may guess." >&2
  echo "    Edit $INDEX, then re-run." >&2
  exit 1
else
  echo "     ✅ already matches"
fi

# ── 3. Make the title page tell the truth ────────────────────────────────────────────────────────
# The stamp is the ONLY way to tell which songbook a device holds with no internet. A new PDF either
# arrives unstamped or carries the previous edition's date; either way it must be re-stamped, and
# stamp-book-date refuses to paint over an existing stamp (it would smudge the title page and every
# gate would still pass). ota-restamp.sh rebuilds page 1 from an unstamped source first.
say "3/4  Stamping the title page"
bash scripts/ota-restamp.sh 2>&1 | sed 's/^/  /'

# ── 4. Everything else ───────────────────────────────────────────────────────────────────────────
say "4/4  Publishing"
exec bash scripts/ota-deploy.sh ${PASSTHRU[@]+"${PASSTHRU[@]}"}

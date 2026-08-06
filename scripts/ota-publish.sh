#!/usr/bin/env bash
#
# ota-publish.sh — THE ONE COMMAND. Give it a new songbook PDF; it reaches the iPads.
#
#   scripts/ota-publish.sh ~/Downloads/nuevo.pdf
#
# That is the whole procedure. It installs the PDF, squares the song index with it, renders the
# book, runs every gate, deploys to signovivo.com, commits the release record, and arms the fleet.
# No Xcode, no TestFlight, no App Store — the binary on every device stays exactly where it is and
# only the book moves.
#
# Your PDF ships as your PDF. Nothing here grafts a page, deletes a page, or draws on one.
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
    -h|--help) sed -n '2,19p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    -*) echo "unknown arg: $1" >&2; exit 2;;
    *) SRC="$1"; shift;;
  esac
done

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

# ── 1. Take the new book ─────────────────────────────────────────────────────────────────────────
if [ -n "$SRC" ]; then
  say "1/3  Installing $SRC"
  [ -f "$SRC" ] || { echo "✖ no such file: $SRC" >&2; exit 1; }
  NEW_PAGES=$(pdfinfo "$SRC" 2>/dev/null | awk '/^Pages/{print $2}') || true
  [ -n "${NEW_PAGES:-}" ] || { echo "✖ $SRC is not a readable PDF (pdfinfo could not open it)." >&2; exit 1; }
  OLD_PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}' || echo 0)
  echo "     $OLD_PAGES pages  ->  $NEW_PAGES pages"
  cp "$SRC" "$BOOK"
else
  say "1/3  No PDF given — publishing the book already in the repo"
  NEW_PAGES=$(pdfinfo "$BOOK" 2>/dev/null | awk '/^Pages/{print $2}')
  echo "     $NEW_PAGES pages"
fi

# ── 2. Keep the song index honest ────────────────────────────────────────────────────────────────
# A page is not a song. songIndex is a hand-maintained [song, page] list (web/build.mjs reads it),
# and check-book-consistency fails the build if a song points past the last page. Both directions
# are mechanical: a book that GREW gets [N, N] appended, a book that SHRANK gets every entry past
# the last page dropped. Neither is a judgement call, so neither stops the run.
#
# This step deliberately does NOT decide whether the resulting book is publishable — it only keeps
# the index arithmetically consistent with the PDF in front of it. The additive gate in
# ota-deploy.sh is what refuses a shrink, and it stays. The split is about what is recoverable:
# a wrong PDF here costs `git checkout assets/songbook.pdf`, while a shrink that reaches the iPads
# strands offline copies inside a church with no internet. Cheap-to-undo gets out of your way;
# impossible-to-undo does not.
say "2/3  Song index"
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
  echo "     dropping index entries past page $NEW_PAGES"
  # Rewrites the whole array rather than regexing entries out one at a time, so the 10-per-line
  # shape survives. Scoped to the RAW_SONG_INDEX literal: matching [n, n] across the entire file
  # would also chew on anything else bracket-shaped that lands in here later.
  node -e '
    const fs=require("fs"); const [file,max]=process.argv.slice(1);
    const s=fs.readFileSync(file,"utf8");
    const block=/(const RAW_SONG_INDEX = \[)[\s\S]*?(\n\];)/;
    if(!block.test(s)){ console.error("  ✖ could not find the RAW_SONG_INDEX literal in "+file); process.exit(1); }
    const keep=[...s.match(block)[0].matchAll(/\[(\d+),\s*(\d+)\]/g)]
      .map(x=>[+x[1],+x[2]]).filter(([,p])=>p<=+max);
    const rows=[]; for(let i=0;i<keep.length;i+=10)
      rows.push("  "+keep.slice(i,i+10).map(([g,p])=>`[${g}, ${p}]`).join(", ")+",");
    fs.writeFileSync(file, s.replace(block, "$1\n"+rows.join("\n")+"$2"));
    process.stdout.write(String(keep.length));
  ' "$INDEX" "$NEW_PAGES" >/tmp/ota-index-kept || exit 1
  echo "     ✅ index now covers $NEW_PAGES ($(cat /tmp/ota-index-kept) songs kept)"
else
  echo "     ✅ already matches"
fi

# ── 3. Everything else ───────────────────────────────────────────────────────────────────────────
# There is no stamping step. Your PDF ships as your PDF — this script does not graft, delete or
# draw on a single page of it. (A stage here used to overlay a "5 de agosto de 2026 · 373 páginas"
# line on page 1 as an offline staleness check. It rebuilt page 1 from a hardcoded commit of a
# filename that no longer existed, and the revision it found was already stamped, so it welded a
# 371-page cover onto a 373-page book, wrote that over assets/songbook.pdf, and only then failed.
# Every publish went through it. If you want a date on the title page, put it there in whatever
# makes the PDF — that is one place that cannot silently disagree with the book.)
say "3/3  Publishing"
exec bash scripts/ota-deploy.sh ${PASSTHRU[@]+"${PASSTHRU[@]}"}

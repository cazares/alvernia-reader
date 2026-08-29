import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..");

// The old committed `assets/standard/*.json` caches were RETIRED (they duplicated build
// output and drifted). `src/alverniaManual2SongIndex.js` — the hand-maintained [song → page]
// index — is what replaced them, and the header this file used to carry said `web/build.mjs`
// reads it to render pages and derive totalPages.
//
// That stopped being true on 2026-08-05. web/build.mjs:713 now DERIVES the index from the
// rendered page count (`song n = page n`), and scripts/check-book-consistency.mjs says so in its
// own header: the file is "frozen at 317 pairs / max page 373 forever while the shipped book
// moves freely." Correcting that claim here matters, because it is what made the assertions below
// worthless — they were written as if guarding the live offline bundle, so nobody looked twice at
// how little they checked.
//
// The file still has one live consumer: scripts/clean-header-boxes.py takes it as the default
// `--index` and walks `for song, pageno in canon: pdf.pages[pageno - 1]`, editing assets/songbook.pdf
// IN PLACE. So a corrupted index is not inert — remap every pair to page 1 and that script whitens
// the same page 317 times and leaves 316 pages of the shipped songbook untouched; truncate the list
// and it silently cleans only the pages that survived. Those are exactly the two corruptions the old
// assertions let through, and they are what these now catch.
const SONG_INDEX_PATH = path.join(ROOT, "src", "alverniaManual2SongIndex.js");

// The frozen shape, from the file's own history and check-book-consistency.mjs's header. These are
// pins, not guesses: the index is frozen by decision, so any movement here is either corruption or
// a deliberate revival that should update this test on purpose.
const EXPECTED_PAIRS = 317;
const EXPECTED_FIRST = { song: 2, page: 2 };
const EXPECTED_LAST = { song: 373, page: 373 };
// The single deliberate irregularity in the whole book, also recorded in docs/app-contracts.md:87.
const EXPECTED_IRREGULAR = { song: 347, page: 346 };

// Parse the [song, page] pairs straight out of the source the same way clean-header-boxes.py:119
// does, so the literal itself is checked and not just whatever the export happened to build.
const parsePairs = () => {
  const src = fs.readFileSync(SONG_INDEX_PATH, "utf8");
  return [...src.matchAll(/\[(\d+),\s*(\d+)\]/g)].map((m) => [Number(m[1]), Number(m[2])]);
};

test("canonical standard song index exists and has song→page entries", async () => {
  assert.ok(fs.existsSync(SONG_INDEX_PATH), `Missing canonical song index: ${SONG_INDEX_PATH}`);

  // What the old assertions missed: `pairs.length > 0` plus a positive-integer loop. Both survive
  // remapping every song to page 1, and both survive throwing away 307 of the 317 pairs — the two
  // mutations that actually destroy the index. Counting, ordering and identity are what separate a
  // real index from ten entries all pointing at page 1.
  const pairs = parsePairs();
  const { ALVERNIA_MANUAL_2_SONG_INDEX: index } = await import("../src/alverniaManual2SongIndex.js");

  // The exported object and the literal must describe the same book. A pair commented out of the
  // array, or a map step that drops entries, shows up here rather than in the loft on a Sunday.
  assert.equal(index.length, pairs.length, "the exported index and the source literal disagree");
  assert.equal(pairs.length, EXPECTED_PAIRS,
    `the song index holds ${pairs.length} pairs, not the frozen ${EXPECTED_PAIRS} — it was truncated or grown`);

  assert.ok(Object.isFrozen(index), "the exported index must stay frozen — a caller could rewrite it");
  assert.ok(Object.isFrozen(index[0]), "each entry must stay frozen");

  const irregular = [];
  for (let i = 0; i < index.length; i += 1) {
    const { song, page } = index[i];
    assert.ok(Number.isInteger(song) && song > 0, `song number must be a positive integer, got ${song}`);
    assert.ok(Number.isInteger(page) && page > 0, `page number must be a positive integer, got ${page}`);
    assert.deepEqual([song, page], pairs[i], `entry ${i} does not match the source literal`);
    if (page !== song) irregular.push({ song, page });
    if (i > 0) {
      // Strictly increasing on BOTH columns. Equal or descending means a duplicate song number or
      // a page walked backwards — either one sends clean-header-boxes.py at the wrong page twice.
      assert.ok(song > index[i - 1].song,
        `song numbers must strictly increase: ${index[i - 1].song} then ${song} at entry ${i}`);
      assert.ok(page > index[i - 1].page,
        `pages must strictly increase: ${index[i - 1].page} then ${page} at entry ${i}`);
    }
  }

  // A song number IS its page number in this book, with exactly one recorded exception. Remapping
  // every pair to page 1 breaks this on entry 1; so does any wholesale renumbering.
  assert.deepEqual(irregular, [EXPECTED_IRREGULAR],
    "the song→page mapping is no longer the identity apart from the one recorded exception");

  assert.deepEqual({ song: index[0].song, page: index[0].page }, EXPECTED_FIRST, "the index no longer starts at song 2");
  assert.deepEqual({ song: index.at(-1).song, page: index.at(-1).page }, EXPECTED_LAST, "the index no longer ends at song 373");
});

test("standard song index derives a valid totalPages floor (max page is a positive integer)", () => {
  const pairs = parsePairs();
  const maxPage = pairs.reduce((m, [, p]) => Math.max(m, p), 0);
  // Any renderer of this index must have at least maxPage pages, else a song points past the end.
  // The old assertion accepted any positive integer below 100000, so a book flattened to page 1 and
  // a book cut down to ten entries both read as a healthy floor. The floor is a known number.
  assert.ok(Number.isInteger(maxPage) && maxPage > 0, "max indexed page must be a positive integer");
  assert.equal(maxPage, EXPECTED_LAST.page,
    `max indexed page is ${maxPage}, not the frozen ${EXPECTED_LAST.page} — the index shrank or was renumbered`);
  // The highest page must be the LAST one listed, not buried mid-list behind a backwards jump.
  assert.deepEqual(pairs.at(-1), [EXPECTED_LAST.song, EXPECTED_LAST.page], "the last pair is not the highest page");
});

test("the retired assets/standard/*.json caches stay retired (build is the source of truth)", () => {
  // Regression guard for the 2026-07-05 refactor (commit 87ee5a39): the committed JSON caches
  // were removed in favor of build output. If they reappear, they will drift again — the book
  // structure must come only from the canonical index above + web/build.mjs.
  const retired = path.join(ROOT, "assets", "standard");
  assert.ok(!fs.existsSync(retired), `assets/standard/ was retired — do not re-commit generated book caches (${retired})`);
});

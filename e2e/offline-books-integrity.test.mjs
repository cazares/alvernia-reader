import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..");

// The old committed `assets/standard/*.json` caches were RETIRED (they duplicated build
// output and drifted). The single source of truth for the standard book's structure is now
// `src/alverniaManual2SongIndex.js` — the canonical [song → page] index that `web/build.mjs`
// reads to render pages + derive totalPages, and that `scripts/check-book-consistency.mjs`
// validates against the PDF. These tests guard THAT source so an empty / corrupt index (which
// would break the offline bundle for everyone) fails CI instead of shipping.
const SONG_INDEX_PATH = path.join(ROOT, "src", "alverniaManual2SongIndex.js");

// Parse the [song, page] pairs the same way web/build.mjs does (regex over the source, no
// import assumptions) so this test pins EXACTLY what the build consumes.
const parsePairs = () => {
  const src = fs.readFileSync(SONG_INDEX_PATH, "utf8");
  return [...src.matchAll(/\[(\d+),\s*(\d+)\]/g)].map((m) => [Number(m[1]), Number(m[2])]);
};

test("canonical standard song index exists and has song→page entries", () => {
  assert.ok(fs.existsSync(SONG_INDEX_PATH), `Missing canonical song index: ${SONG_INDEX_PATH}`);
  const pairs = parsePairs();
  assert.ok(pairs.length > 0, "the canonical song index must contain [song, page] pairs");
  // Every entry must be a pair of positive integers — a zero/negative/NaN would render a
  // bogus page or clamp wrong.
  for (const [song, page] of pairs) {
    assert.ok(Number.isInteger(song) && song > 0, `song number must be a positive integer, got ${song}`);
    assert.ok(Number.isInteger(page) && page > 0, `page number must be a positive integer, got ${page}`);
  }
});

test("standard song index derives a valid totalPages floor (max page is a positive integer)", () => {
  const pairs = parsePairs();
  const maxPage = pairs.reduce((m, [, p]) => Math.max(m, p), 0);
  // web/build.mjs sets totalPages = rendered page count, which must be >= the highest indexed
  // page (else a song would point past the end). A positive max page is the committed floor.
  assert.ok(Number.isInteger(maxPage) && maxPage > 0, "max indexed page must be a positive integer");
  // Sanity ceiling — the standard manual is ~371 pages; guard against an absurd/corrupt value.
  assert.ok(maxPage < 100000, `max indexed page ${maxPage} is implausibly large — likely corrupt`);
});

test("the retired assets/standard/*.json caches stay retired (build is the source of truth)", () => {
  // Regression guard for the 2026-07-05 refactor (commit 87ee5a39): the committed JSON caches
  // were removed in favor of build output. If they reappear, they will drift again — the book
  // structure must come only from the canonical index above + web/build.mjs.
  const retired = path.join(ROOT, "assets", "standard");
  assert.ok(!fs.existsSync(retired), `assets/standard/ was retired — do not re-commit generated book caches (${retired})`);
});

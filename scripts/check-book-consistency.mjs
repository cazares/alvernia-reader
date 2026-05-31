#!/usr/bin/env node
/**
 * Guard against the build-325/327 "song N unreachable" bug class.
 *
 * The app renders `Array.from({ length: totalPages })` and clamps navigation to
 * `totalPages` (PdfReaderApp.tsx). So if assets/standard/pages.json#totalPages is
 * LESS than the actual bundled PDF page count, every page beyond it is invisible
 * AND unreachable by song number — no matter what's in the PDF or the song index.
 *
 * This check FAILS the build when the page count drifts from the PDF, forcing the
 * data (pages.json / song-titles.json / song-search-index.json / alverniaManual2SongIndex)
 * to be updated whenever the songbook PDF changes.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const pagesJsonPath = path.join(root, "assets/standard/pages.json");
const pdfPath = path.join(root, "assets/alvernia_manual_2.pdf");

const pages = JSON.parse(fs.readFileSync(pagesJsonPath, "utf8"));
const declared = Number(pages.totalPages);
const songIndex = Array.isArray(pages.songIndex) ? pages.songIndex : [];
const maxIndexedPage = songIndex.reduce((m, e) => Math.max(m, Number(e?.page) || 0), 0);

const info = spawnSync("pdfinfo", [pdfPath], { encoding: "utf8" });
if (info.status !== 0) {
  console.warn("⚠️  check-book-consistency: pdfinfo unavailable — skipping PDF page-count check (install poppler to enable).");
  process.exit(0);
}
const match = info.stdout.match(/^Pages:\s+(\d+)/m);
const actual = match ? Number(match[1]) : NaN;

let ok = true;
if (!Number.isFinite(actual)) {
  console.warn("⚠️  check-book-consistency: could not read PDF page count — skipping.");
  process.exit(0);
}
if (declared !== actual) {
  console.error(`❌ pages.json totalPages=${declared} but ${path.basename(pdfPath)} has ${actual} pages.`);
  console.error("   Pages beyond totalPages are NOT rendered and NOT jump-reachable.");
  console.error("   Fix: set assets/standard/pages.json totalPages to " + actual + " and add any new song(s)");
  console.error("   to song-titles.json, song-search-index.json, and src/alverniaManual2SongIndex.js.");
  ok = false;
}
if (maxIndexedPage > declared) {
  console.error(`❌ pages.json songIndex references page ${maxIndexedPage} > totalPages ${declared}.`);
  ok = false;
}
if (Number.isFinite(actual) && maxIndexedPage && maxIndexedPage < actual) {
  console.warn(`⚠️  Last indexed song is on page ${maxIndexedPage} but the PDF has ${actual} pages — the final page(s) may not be jump-reachable by song number.`);
}

if (!ok) process.exit(1);
console.log(`✅ book consistency OK — pages.json totalPages=${declared} matches the PDF (${actual} pages); songIndex maxPage=${maxIndexedPage}.`);

#!/usr/bin/env node
/**
 * Guard against the build-325/327 "song N unreachable" bug class.
 *
 * The app renders one page slot up to totalPages and clamps navigation to it.
 * totalPages is derived at build time from the actual rendered page count
 * (web/build.mjs: `totalPages: pageFiles.length`), so it cannot drift from the PDF.
 * The remaining risk is the SONG INDEX pointing a song at a page the PDF does not
 * have — then jump-by-number strands a user on a blank/clamped page during Mass.
 *
 * This validates the canonical song index (src/alverniaManual2SongIndex.js) — the
 * single source web/build.mjs reads — against the shipped PDF, and FAILS the build
 * if any song points beyond the last page.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const songIndexPath = path.join(root, "src/alverniaManual2SongIndex.js");
const pdfPath = path.join(root, "assets/signo_vivo_372.pdf");

const src = fs.readFileSync(songIndexPath, "utf8");
const pairs = [...src.matchAll(/\[(\d+),\s*(\d+)\]/g)].map((m) => [Number(m[1]), Number(m[2])]);
if (!pairs.length) {
  console.error(`❌ check-book-consistency: no [song, page] pairs found in ${path.basename(songIndexPath)}.`);
  process.exit(1);
}
const songCount = pairs.length;
const maxIndexedPage = pairs.reduce((m, [, p]) => Math.max(m, Number(p) || 0), 0);

const info = spawnSync("pdfinfo", [pdfPath], { encoding: "utf8" });
if (info.status !== 0) {
  console.warn("⚠️  check-book-consistency: pdfinfo unavailable — skipping PDF page-count check (install poppler to enable).");
  process.exit(0);
}
const match = info.stdout.match(/^Pages:\s+(\d+)/m);
const actual = match ? Number(match[1]) : NaN;
if (!Number.isFinite(actual)) {
  console.warn("⚠️  check-book-consistency: could not read PDF page count — skipping.");
  process.exit(0);
}

let ok = true;
if (maxIndexedPage > actual) {
  console.error(`❌ song index references page ${maxIndexedPage} but ${path.basename(pdfPath)} has only ${actual} pages.`);
  console.error("   That song is not rendered and not jump-reachable by number.");
  console.error("   Fix: correct the page in src/alverniaManual2SongIndex.js, or update the PDF.");
  ok = false;
}
if (maxIndexedPage && maxIndexedPage < actual) {
  console.warn(`⚠️  Last indexed song is on page ${maxIndexedPage} but the PDF has ${actual} pages — the final page(s) may not be jump-reachable by song number.`);
}

if (!ok) process.exit(1);
console.log(`✅ book consistency OK — ${songCount} songs, max indexed page ${maxIndexedPage} ≤ PDF pages ${actual}.`);

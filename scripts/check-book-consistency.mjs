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
const pdfPath = path.join(root, "assets/songbook.pdf");

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

// ── Three-way cross-check: PDF ↔ render ↔ manifest ───────────────────────────
// Every integrity check in the distribution design hashes the artifact against itself, so a
// self-consistent WRONG book passes all of them (red team A6). The one genuinely independent
// measurement available is pdfinfo's page count, taken from the SOURCE PDF rather than from the
// render. Requiring PDF == rendered images == manifest.totalPages means a render that silently
// dropped pages, or a manifest describing a different build, is a mismatch instead of a consistent
// lie. Skipped (not failed) when web/dist has not been built — this script also runs as a preflight
// before the build exists.
const distPagesDir = path.join(root, "web/dist/books/standard/pages");
const manifestPath = path.join(root, "web/dist/bundle-manifest.json");
if (fs.existsSync(distPagesDir) && fs.existsSync(manifestPath)) {
  const rendered = fs.readdirSync(distPagesDir).filter((f) => /^page-\d+\.webp$/.test(f)).length;
  let manifest = null;
  try {
    manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  } catch (e) {
    console.error(`❌ web/dist/bundle-manifest.json is unparseable: ${e.message}`);
    ok = false;
  }
  if (manifest) {
    if (rendered !== actual) {
      console.error(`❌ the PDF has ${actual} pages but ${rendered} page image(s) were rendered.`);
      console.error("   A page that exists in the book but not in the bundle is a blank during Mass.");
      ok = false;
    }
    if (Number(manifest.totalPages) !== actual) {
      console.error(`❌ bundle-manifest.json says totalPages=${manifest.totalPages}, the PDF has ${actual}.`);
      ok = false;
    }
    if (Number(manifest.sourcePdfPages) !== actual) {
      console.error(`❌ bundle-manifest.json records sourcePdfPages=${manifest.sourcePdfPages}, pdfinfo now reads ${actual} — the manifest describes a DIFFERENT PDF than the one on disk.`);
      ok = false;
    }
    if (ok) {
      console.log(`✅ PDF ↔ render ↔ manifest agree — ${actual} pages, ${manifest.bookVersion}.`);
    }
  }
}

if (!ok) process.exit(1);
console.log(`✅ book consistency OK — ${songCount} songs, max indexed page ${maxIndexedPage} ≤ PDF pages ${actual}.`);

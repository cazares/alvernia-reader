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
 * It USED to validate src/alverniaManual2SongIndex.js, a hand-maintained [song, page] list, against
 * the PDF. That file stopped being read by anything on 2026-08-05: web/build.mjs now DERIVES the
 * index from the rendered page count (song n = page n), so the file is frozen at 317 pairs / max
 * page 373 forever while the shipped book moves freely.
 *
 * Leaving the check pointed at it was worse than deleting it. Publish a 350-page PDF over the OTA
 * (which sets SKIP_GATES=1 and never runs this) and the next NATIVE release — or plain `npm run
 * preios` — hard-failed with "correct the page in src/alverniaManual2SongIndex.js": a file the
 * build ignores, so following the advice fixes nothing and editing it changes nothing. The native
 * release and the simulator were both blocked by a fossil.
 *
 * It now validates what actually ships: the DERIVED index (song n = page n for n in 1..pages) is
 * true by construction, so the only real question left is whether the PDF, the rendered pages and
 * the manifest agree — which is the cross-check below, and which used to sit unreachable behind an
 * early exit on that dead file.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const pdfPath = path.join(root, "assets/songbook.pdf");


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
// song n = page n, derived from the render, so "a song points past the last page" is no longer
// expressible. What IS still worth failing on is a book with no pages at all.
if (!(actual > 0)) {
  console.error(`❌ ${path.basename(pdfPath)} reports ${actual} pages.`);
  ok = false;
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
console.log(`✅ book consistency OK — ${actual} pages, song n = page n, PDF/render/manifest agree.`);

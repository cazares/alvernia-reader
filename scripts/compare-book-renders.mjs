#!/usr/bin/env node
/**
 * Compare two editions of the songbook AT THE EXACT BUILD SETTINGS and report which
 * rendered pages actually change.
 *
 * WHY THIS EXISTS. The additive-only rule (docs/choir-pdf-distribution-plan.md §2) is what
 * keeps every stale offline copy valid forever, and it has already been violated once in
 * production: build 377 / PR #257 re-rendered ~290 pages in place under unchanged
 * `page-NNN.webp` filenames. `pdfinfo` cannot see that — the page COUNT is identical in a
 * pure in-place rewrite — so "372 -> 373 pages" is not evidence that the other 372 pages
 * were left alone.
 *
 * The only honest check is to render both PDFs through the SAME pipeline web/build.mjs uses
 * (pdftoppm -r <DPI> -> cwebp -q <Q>) and hash the results. Identical sha256 means a device
 * that already cached that page has nothing to re-download and nothing to lose. Anything else
 * is an in-place correction, which is legal since PR #278 (BOOK_VERSION busts the page cache)
 * but must be a DECISION, not a surprise.
 *
 * Renders a sample by default because a full 373-page double render is ~10 minutes and the
 * failure mode this catches — a whole-book re-encode — shows up in any sample. Use --all when
 * the change is supposed to be surgical and you need every page proven.
 *
 * Usage:
 *   node scripts/compare-book-renders.mjs --a <old.pdf> --b <new.pdf>
 *   node scripts/compare-book-renders.mjs --a <old.pdf> --b <new.pdf> --pages 1,2,50,371
 *   node scripts/compare-book-renders.mjs --a <old.pdf> --b <new.pdf> --all
 *
 * Exit codes: 0 = compared cleanly (read the report), 1 = could not compare.
 * It deliberately does NOT fail on a difference — "page 1 changed" is the expected result
 * when the edition stamp is refreshed. The report is for a human to accept.
 *
 * Requires: poppler (pdftoppm, pdfinfo) + cwebp — the same tools web/build.mjs shells out to.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const argv = process.argv.slice(2);
const arg = (name, fallback = null) => {
  const i = argv.indexOf(name);
  return i === -1 ? fallback : (argv[i + 1] ?? fallback);
};
const flag = (name) => argv.includes(name);

// Mirror web/build.mjs's knobs, including its env overrides, so the comparison is done at the
// settings this repo will actually publish with — not at a default that happens to agree today.
const DPI = Number.parseInt(process.env.ALVERNIA_PDF_RENDER_DPI ?? "", 10) || 115;
const QUALITY = Number.parseInt(process.env.ALVERNIA_PDF_WEBP_QUALITY ?? "", 10) || 60;

const aRel = arg("--a");
const bRel = arg("--b");
if (!aRel || !bRel) {
  console.error("Usage: --a <old.pdf> --b <new.pdf> [--pages 1,2,50] [--all]");
  process.exit(1);
}
const aPath = path.resolve(root, aRel);
const bPath = path.resolve(root, bRel);
for (const p of [aPath, bPath]) {
  if (!fs.existsSync(p)) {
    console.error(`No such PDF: ${p}`);
    process.exit(1);
  }
}
for (const bin of ["pdftoppm", "pdfinfo", "cwebp"]) {
  if (spawnSync("which", [bin]).status !== 0) {
    console.error(`Missing required tool: ${bin}`);
    process.exit(1);
  }
}

const pageCount = (pdf) => {
  const info = spawnSync("pdfinfo", [pdf], { encoding: "utf8" });
  const m = info.status === 0 ? info.stdout.match(/^Pages:\s+(\d+)/m) : null;
  return m ? Number(m[1]) : NaN;
};
const aPages = pageCount(aPath);
const bPages = pageCount(bPath);
if (!Number.isFinite(aPages) || !Number.isFinite(bPages)) {
  console.error("Could not read page counts.");
  process.exit(1);
}

// Only pages present in BOTH can be compared; pages beyond the shorter book are new by
// definition and are reported separately rather than counted as differences.
const shared = Math.min(aPages, bPages);
const parseList = (s) =>
  String(s).split(",").map((t) => Number.parseInt(t.trim(), 10)).filter((n) => Number.isFinite(n) && n >= 1);

let pages;
if (flag("--all")) {
  pages = Array.from({ length: shared }, (_, i) => i + 1);
} else if (arg("--pages")) {
  pages = parseList(arg("--pages"));
} else {
  // Default sample: both ends, both boundaries, and an even spread through the middle. The
  // first and last shared pages matter most — the stamp lives on page 1 and an append lands
  // next to the old last page.
  const spread = [1, 2, 3];
  for (let i = 1; i <= 9; i += 1) spread.push(Math.max(1, Math.round((shared * i) / 10)));
  spread.push(shared - 2, shared - 1, shared);
  pages = [...new Set(spread)].filter((n) => n >= 1 && n <= shared).sort((x, y) => x - y);
}
const outOfRange = pages.filter((n) => n > shared);
pages = pages.filter((n) => n <= shared);
if (!pages.length) {
  console.error("No comparable pages selected.");
  process.exit(1);
}

// Render ONE page to webp through the exact build path and return its sha256.
const renderHash = (pdf, pageNo, tmp, tag) => {
  const prefix = path.join(tmp, `${tag}-${pageNo}`);
  const r = spawnSync(
    "pdftoppm",
    ["-png", "-r", String(DPI), "-f", String(pageNo), "-l", String(pageNo), pdf, prefix],
    { stdio: "pipe" },
  );
  if (r.status !== 0) throw new Error(`pdftoppm failed on ${tag} page ${pageNo}`);
  // pdftoppm chooses its own zero-padding width; glob for whatever it produced.
  const base = path.basename(prefix);
  const png = fs.readdirSync(tmp).find((f) => f.startsWith(`${base}-`) && f.endsWith(".png"));
  if (!png) throw new Error(`no PNG produced for ${tag} page ${pageNo}`);
  const pngPath = path.join(tmp, png);
  const webpPath = `${pngPath}.webp`;
  const w = spawnSync("cwebp", ["-quiet", "-q", String(QUALITY), pngPath, "-o", webpPath], { stdio: "pipe" });
  if (w.status !== 0 || !fs.existsSync(webpPath)) throw new Error(`cwebp failed on ${tag} page ${pageNo}`);
  const hash = crypto.createHash("sha256").update(fs.readFileSync(webpPath)).digest("hex");
  fs.unlinkSync(pngPath);
  fs.unlinkSync(webpPath);
  return hash;
};

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "sv-cmp-"));
console.log(`Comparing at the build settings: ${DPI} DPI, WebP q${QUALITY}`);
console.log(`  A: ${path.relative(root, aPath)}  (${aPages} pages)`);
console.log(`  B: ${path.relative(root, bPath)}  (${bPages} pages)`);
console.log(`  comparing ${pages.length} of ${shared} shared page(s)${flag("--all") ? " (ALL)" : " (sample)"}\n`);

const differing = [];
try {
  for (const n of pages) {
    const ha = renderHash(aPath, n, tmp, "a");
    const hb = renderHash(bPath, n, tmp, "b");
    const same = ha === hb;
    if (!same) differing.push(n);
    console.log(`  page ${String(n).padStart(4)}  ${same ? "= identical" : "≠ DIFFERS"}   ${ha.slice(0, 12)} ${same ? "  " : "vs"} ${same ? "" : hb.slice(0, 12)}`);
  }
} finally {
  fs.rmSync(tmp, { recursive: true, force: true });
}

console.log();
if (bPages > aPages) {
  const added = Array.from({ length: bPages - aPages }, (_, i) => aPages + i + 1);
  console.log(`➕ ${added.length} page(s) added by B: ${added.join(", ")} — purely additive, no cached page invalidated.`);
} else if (bPages < aPages) {
  console.log(`⚠️  B is SHORTER than A (${bPages} < ${aPages}). Pages ${bPages + 1}..${aPages} disappear — this breaks jump-by-number for anything indexed there.`);
}
if (outOfRange.length) {
  console.log(`(skipped ${outOfRange.length} requested page(s) beyond the shared range: ${outOfRange.join(", ")})`);
}
if (differing.length === 0) {
  console.log(`✅ every compared page renders byte-identically — nothing changed IN PLACE.`);
} else {
  console.log(`⚠️  ${differing.length} page(s) changed IN PLACE: ${differing.join(", ")}`);
  console.log(`   Legal since PR #278 (BOOK_VERSION busts the page cache), but every already-cached`);
  console.log(`   device re-downloads these. Confirm each one is intended before publishing.`);
}

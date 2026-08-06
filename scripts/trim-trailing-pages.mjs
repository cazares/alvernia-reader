#!/usr/bin/env node
/**
 * Remove TRAILING pages from the songbook PDF — the counterpart to append-number-page.mjs.
 *
 * WHY THIS EXISTS. Canary pages get appended to prove an OTA landed (that is what
 * append-number-page.mjs is for), and until 2026-08-05 there was no way back: the additive rule
 * refused every shrink, so three throwaway pages could only be removed by a TestFlight round. That
 * made the songbook a one-way ratchet, which is the opposite of what an over-the-air book is for.
 *
 * The rule is now "a book may shrink, but never below the last page a song points at" — enforced in
 * three places that must agree:
 *
 *   src/bookUpdate.js         verifyStaged      (the DEVICE refuses a shrink that strands a song)
 *   scripts/additive-gate.mjs                   (the RELEASE refuses to publish one)
 *   this script                                 (the SOURCE refuses to create one)
 *
 * Refusing here is what makes the other two never fire in anger: the bad PDF is never built.
 *
 * Usage:
 *   node scripts/trim-trailing-pages.mjs --to 371
 *   node scripts/trim-trailing-pages.mjs --to 371 --pdf assets/songbook.pdf
 *   node scripts/trim-trailing-pages.mjs --to 371 --dry-run
 *
 * Requires: qpdf + poppler (pdfinfo) — the same set as append-number-page.mjs.
 */
import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const rootDir = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const arg = (name, fallback = null) => {
  const i = process.argv.indexOf(name);
  return i !== -1 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
};
const has = (name) => process.argv.includes(name);

const pdfPath = path.resolve(rootDir, arg("--pdf", "assets/songbook.pdf"));
const target = Number(arg("--to"));
const dryRun = has("--dry-run");

const die = (msg) => {
  console.error(`✖ ${msg}`);
  process.exit(1);
};

if (!Number.isFinite(target) || target < 1) die("--to <pageCount> is required (e.g. --to 371)");
if (!fs.existsSync(pdfPath)) die(`no such PDF: ${pdfPath}`);

const pageCount = (p) => {
  const out = execFileSync("pdfinfo", [p], { encoding: "utf8" });
  const m = /^Pages:\s+(\d+)$/m.exec(out);
  if (!m) die(`pdfinfo could not read a page count from ${p}`);
  return Number(m[1]);
};

const current = pageCount(pdfPath);
if (target === current) {
  console.log(`✓ already ${current} pages — nothing to do.`);
  process.exit(0);
}
if (target > current) die(`--to ${target} is MORE than the ${current} pages present. Use scripts/append-number-page.mjs to grow a book.`);

// ── THE FLOOR. Never trim below the last page a song points at. ──────────────
// Read it from the built manifest when present (it is the same songPages array the device and the
// additive gate use), and fall back to the shipped song index. Fail CLOSED: if the floor cannot be
// established, refuse rather than guess, because guessing wrong strands a song mid-Mass.
const readFloor = () => {
  const manifestPath = path.join(rootDir, "web/dist/bundle-manifest.json");
  if (fs.existsSync(manifestPath)) {
    try {
      const m = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
      if (Array.isArray(m.songPages) && m.songPages.length) {
        const pages = m.songPages
          .filter((e) => Array.isArray(e) && e.length >= 2)
          .map((e) => Number(e[1]))
          .filter(Number.isFinite);
        if (pages.length) return { floor: Math.max(...pages), source: "web/dist/bundle-manifest.json" };
      }
    } catch {
      /* fall through to the index */
    }
  }
  const idxPath = path.join(rootDir, "web/dist/books/standard/pages.json");
  if (fs.existsSync(idxPath)) {
    try {
      const idx = JSON.parse(fs.readFileSync(idxPath, "utf8"));
      const nums = JSON.stringify(idx).match(/"page"\s*:\s*(\d+)/g);
      if (nums && nums.length) {
        const pages = nums.map((s) => Number(s.replace(/\D/g, ""))).filter(Number.isFinite);
        if (pages.length) return { floor: Math.max(...pages), source: "web/dist/books/standard/pages.json" };
      }
    } catch {
      /* fall through */
    }
  }
  return { floor: null, source: null };
};

const { floor, source } = readFloor();
if (floor == null) {
  die(
    "could not determine the last song page — run `node web/build.mjs` first so web/dist exists.\n" +
      "  Refusing to trim a book whose song index cannot be read (fail closed).",
  );
}
console.log(`  last song page: ${floor}  (from ${source})`);

if (target < floor) {
  die(
    `--to ${target} would strand songs: a song still points at page ${floor}.\n` +
      `  Typing that number would land on nothing. The lowest safe target is ${floor}.`,
  );
}

const dropping = current - target;
console.log(`  ${current} → ${target} pages  (dropping ${dropping} trailing page${dropping === 1 ? "" : "s"}: ${target + 1}-${current})`);
console.log(`  every page a song points at (1-${floor}) is kept.`);

if (dryRun) {
  console.log("✓ --dry-run: nothing written.");
  process.exit(0);
}

// qpdf writes to a temp file first so an interrupted run can never leave a truncated songbook in
// place — the source PDF is the one artifact with no other copy in the repo.
const tmp = `${pdfPath}.trim.tmp`;
try {
  execFileSync("qpdf", [pdfPath, "--pages", pdfPath, `1-${target}`, "--", tmp], { stdio: "inherit" });
} catch (e) {
  fs.rmSync(tmp, { force: true });
  die(`qpdf failed: ${e.message}`);
}

const got = pageCount(tmp);
if (got !== target) {
  fs.rmSync(tmp, { force: true });
  die(`qpdf produced ${got} pages, expected ${target} — refusing to install it.`);
}
fs.renameSync(tmp, pdfPath);
console.log(`✅ ${pdfPath} is now ${got} pages.`);
console.log("   Next: node web/build.mjs   (then release.sh — the additive gate checks the same floor)");

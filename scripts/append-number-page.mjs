#!/usr/bin/env node
/**
 * Append a page showing one huge number to the END of the songbook PDF.
 *
 * This is the CANARY / REHEARSAL page maker. The original "372" canary page (PR #278) proved
 * the additive-update pipeline end to end; this script recreates that capability as a tool
 * instead of a one-off (the original was made ad hoc and never committed — this is the
 * §-scripts-get-committed correction). Typical use: rehearse an OTA book update by appending
 * page N+1 to the current book, deploying to STAGING, and watching a cached client converge.
 *
 * The page renders the number at a size readable across a room: jumping to an out-of-range
 * song lands on the last page (resolveSongPage clamps to totalPages), so the number on that
 * page tells you at a glance which book a device actually holds.
 *
 * Usage:
 *   node scripts/append-number-page.mjs --pdf assets/signo_vivo_372.pdf --number 373
 *   node scripts/append-number-page.mjs --pdf <in> --out <out> --number 373
 *
 * Requires: ghostscript (gs) + qpdf + poppler (pdfinfo) — same set as stamp-book-date.mjs.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const argv = process.argv.slice(2);
const arg = (name, fallback = null) => {
  const i = argv.indexOf(name);
  return i === -1 ? fallback : (argv[i + 1] ?? fallback);
};

const pdfRel = arg("--pdf");
// --number is the original canary mode: one huge digit group, readable across a room.
// --text is the same page with arbitrary lines (split on a literal \n), for a rehearsal marker
// that has to say WHICH update it came from — "[Song 372 goes here]" over "OTA <timestamp>".
// A bare number carries no provenance, and during an OTA rehearsal the whole question is which
// edition a device is holding.
const numberText = String(arg("--number", "")).trim();
const freeText = arg("--text", null);
const lines = freeText !== null
  ? String(freeText).split("\\n").map((s) => s.trim()).filter(Boolean)
  : [numberText];
if (!pdfRel || (freeText === null && !/^\d{1,4}$/.test(numberText)) || !lines.length) {
  console.error("Usage: --pdf <path> (--number <1-4 digits> | --text 'line one\\nline two') [--out <path>]");
  process.exit(2);
}
const pdfPath = path.resolve(root, pdfRel);
const outPath = path.resolve(root, arg("--out", pdfRel));
if (!fs.existsSync(pdfPath)) {
  console.error(`No such PDF: ${pdfPath}`);
  process.exit(1);
}
for (const bin of ["gs", "qpdf", "pdfinfo"]) {
  if (spawnSync("which", [bin]).status !== 0) {
    console.error(`Missing required tool: ${bin}`);
    process.exit(1);
  }
}

// Geometry from the PDF itself so the appended page always matches the book.
const info = spawnSync("pdfinfo", [pdfPath], { encoding: "utf8" });
const pagesBefore = Number((info.stdout.match(/^Pages:\s+(\d+)/m) || [])[1]);
const sizeMatch = info.stdout.match(/^Page size:\s+([\d.]+) x ([\d.]+)/m);
if (!Number.isFinite(pagesBefore) || !sizeMatch) {
  console.error("Could not read page count / size from pdfinfo.");
  process.exit(1);
}
const pageW = Number(sizeMatch[1]);
const pageH = Number(sizeMatch[2]);

// Big, centered, bold — Helvetica-Bold sized so the LONGEST line still fits the page width, then
// capped so a short line does not become absurdly tall on a multi-line page. The whole point is
// legibility across a room: an operator confirms the update by looking, not by reading a hash.
const longest = lines.reduce((a, b) => (b.length > a.length ? b : a), "");
const FONT_SIZE = Math.max(12, Math.min(
  Math.floor(pageW / (Math.max(1, longest.length) * 0.62)),
  Math.floor(pageH / (lines.length * 2.2)),
));
const LINE_GAP = Math.floor(FONT_SIZE * 1.35);
// PostScript string literals: ( ) and \ are the only characters that must be escaped.
const psEscape = (s) => s.replace(/([\\()])/g, "\\$1");
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "sv-append-"));
const psPath = path.join(tmp, "page.ps");
const pagePdf = path.join(tmp, "page.pdf");

fs.writeFileSync(
  psPath,
  `%!PS-Adobe-3.0
<< /PageSize [${pageW} ${pageH}] >> setpagedevice
/Helvetica-Bold findfont ${FONT_SIZE} scalefont setfont
0 setgray
${lines
  .map((line, i) => {
    // Stack the block vertically centred: first line starts half a block above the middle.
    const offset = ((lines.length - 1) / 2 - i) * LINE_GAP;
    const y = (pageH - FONT_SIZE) / 2 + FONT_SIZE * 0.28 + offset;
    return `(${psEscape(line)}) dup stringwidth pop\n${pageW} exch sub 2 div\n${y.toFixed(2)}\nmoveto show`;
  })
  .join("\n")}
showpage
`,
);

const gs = spawnSync(
  "gs",
  ["-q", "-dBATCH", "-dNOPAUSE", "-sDEVICE=pdfwrite",
    `-dDEVICEWIDTHPOINTS=${pageW}`, `-dDEVICEHEIGHTPOINTS=${pageH}`, "-dFIXEDMEDIA",
    `-sOutputFile=${pagePdf}`, psPath],
  { stdio: "inherit" },
);
if (gs.status !== 0) {
  console.error("ghostscript failed to render the number page.");
  process.exit(1);
}

const merged = path.join(tmp, "merged.pdf");
const cat = spawnSync("qpdf", [pdfPath, "--pages", pdfPath, "1-z", pagePdf, "1", "--", merged], { stdio: "inherit" });
if (cat.status !== 0) {
  console.error("qpdf append failed.");
  process.exit(1);
}

// Verify before writing: exactly one page added, geometry unchanged.
const after = spawnSync("pdfinfo", [merged], { encoding: "utf8" });
const pagesAfter = Number((after.stdout.match(/^Pages:\s+(\d+)/m) || [])[1]);
const sizeAfter = after.stdout.match(/^Page size:\s+([\d.]+) x ([\d.]+)/m);
if (pagesAfter !== pagesBefore + 1 || !sizeAfter || Number(sizeAfter[1]) !== pageW || Number(sizeAfter[2]) !== pageH) {
  console.error(`❌ expected ${pagesBefore + 1} pages at ${pageW}x${pageH}; got ${pagesAfter}. Refusing to write.`);
  process.exit(1);
}

fs.copyFileSync(merged, outPath);
fs.rmSync(tmp, { recursive: true, force: true });
console.log(`✅ appended page ${pagesAfter} ("${lines.join(" / ")}") — ${pagesBefore} -> ${pagesAfter} pages, ${pageW}x${pageH} pts`);
console.log(`   -> ${path.relative(root, outPath)}`);

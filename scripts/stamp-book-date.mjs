#!/usr/bin/env node
/**
 * Stamp a human-readable edition line onto page 1 of the songbook PDF.
 *
 *   "1 de agosto de 2026 · 372 páginas"
 *
 * WHY THIS EXISTS. There is no reliable way to tell which songbook a device is actually
 * rendering. The bottom-right build badge shows the native SHELL's build number, not the
 * book's — a device can display "b382" while rendering a book from months earlier. The
 * fleet dashboard reports nativeBuild and webCached, neither of which is the book. And
 * inside the church there is no internet, so nothing can be looked up.
 *
 * A date printed INTO the artifact solves that, and it has one property nothing else has:
 * it reads out the truth even when a cache is stale. A device serving an old book shows
 * the old date. A device that updated shows the new one. The director can check the whole
 * room in seconds — "¿la suya dice agosto?" — with no network and no maintainer present.
 *
 * The page COUNT is on the same line deliberately: a date alone cannot catch a truncated
 * or half-rendered book, which is exactly the failure a partial update would produce.
 *
 * It is stamped into the PDF rather than drawn by the app so that everything downstream
 * inherits it — the rendered page images, the PDF that gets AirDropped as the offline
 * fallback, and any printout. One source of truth, and the fallback copy declares its own
 * age, which is the staleness problem pre-positioned PDFs otherwise have.
 *
 * Bottom-LEFT on purpose: the app draws its build badge in the bottom-right.
 *
 * Usage:
 *   node scripts/stamp-book-date.mjs --pdf assets/songbook.pdf
 *   node scripts/stamp-book-date.mjs --pdf <in> --out <out> --date 2026-08-01
 *
 * Requires: ghostscript (gs) + qpdf + poppler (pdfinfo). All already used by this repo.
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
if (!pdfRel) {
  console.error("Missing --pdf <path>");
  process.exit(2);
}
const pdfPath = path.resolve(root, pdfRel);
const outPath = path.resolve(root, arg("--out", pdfRel));
if (!fs.existsSync(pdfPath)) {
  console.error(`No such PDF: ${pdfPath}`);
  process.exit(1);
}

const need = (bin) => {
  if (spawnSync("which", [bin]).status !== 0) {
    console.error(`Missing required tool: ${bin}`);
    process.exit(1);
  }
};
["gs", "qpdf", "pdfinfo"].forEach(need);

// ── REFUSE TO DOUBLE-STAMP ────────────────────────────────────────────────────────────
// This script OVERLAYS; it does not replace. Running it twice on the same PDF paints the
// second edition line directly on top of the first — same corner, same font, same size —
// producing unreadable overlapping glyphs on the title page of the book the whole choir
// opens. Nothing downstream catches it: the page count is unchanged, the geometry is
// unchanged, and the verification below still passes. It renders as a smudge.
//
// The intended flow (docs: HANDOFF §9) never hits this, because a new PDF from the music
// director arrives unstamped. It bites when an EXISTING shipped book is amended — appending
// a page, say — and needs its count line refreshed. The fix is not to force it through: it
// is to rebuild page 1 from an unstamped source, e.g.
//
//   qpdf --empty --pages <unstamped>.pdf 1 <stamped>.pdf 2-z -- clean.pdf
//
// which keeps every other page byte-identical (verify with scripts/compare-book-renders.mjs).
// Matches BOTH stamp generations: the original date-only form ("2 de agosto de 2026 · 373
// páginas") and the current date+time form ("3 de agosto de 2026, 2:47 p.m. CT · 371 páginas").
// The time group is OPTIONAL on purpose — books stamped before the clock was added must still be
// detected, or this guard silently stops guarding exactly the older books most likely to need a
// re-stamp.
const STAMP_RE = /\d{1,2}\s+de\s+\p{L}+\s+de\s+\d{4}(?:\s*,\s*\d{1,2}:\d{2}\s*[ap]\.?\s*m\.?\s*[A-Z]{2,4})?\s*[·.]\s*\d+\s*p[áa]ginas/iu;
if (spawnSync("which", ["pdftotext"]).status !== 0) {
  console.warn("⚠️  pdftotext not found — could NOT check for an existing stamp. If this PDF is");
  console.warn("    already stamped, the new line will be painted on top of the old one.");
} else {
  const page1 = spawnSync("pdftotext", ["-f", "1", "-l", "1", "-layout", pdfPath, "-"], { encoding: "utf8" });
  const existing = page1.status === 0 ? (page1.stdout.match(STAMP_RE) || [])[0] : null;
  if (existing) {
    console.error(`❌ page 1 is ALREADY stamped: "${existing.trim()}"`);
    console.error("   Stamping again would overlay the new line on top of it — unreadable, and");
    console.error("   nothing downstream would catch it. Rebuild page 1 from an unstamped copy first:");
    console.error("     qpdf --empty --pages <unstamped>.pdf 1 <this>.pdf 2-z -- clean.pdf");
    console.error("   then stamp clean.pdf. Refusing to write.");
    process.exit(1);
  }
}

// ── Book facts, read from the PDF itself so the stamp can never disagree with it ──────
const info = spawnSync("pdfinfo", [pdfPath], { encoding: "utf8" });
if (info.status !== 0) {
  console.error("pdfinfo failed — cannot read the PDF.");
  process.exit(1);
}
const pages = Number((info.stdout.match(/^Pages:\s+(\d+)/m) || [])[1]);
const sizeMatch = info.stdout.match(/^Page size:\s+([\d.]+) x ([\d.]+)/m);
if (!Number.isFinite(pages) || !sizeMatch) {
  console.error("Could not read page count / page size from pdfinfo.");
  process.exit(1);
}
const pageW = Number(sizeMatch[1]);
const pageH = Number(sizeMatch[2]);

// ── The date. Defaults to today; --date YYYY-MM-DD for a reproducible build. ──────────
const MESES = [
  "enero", "febrero", "marzo", "abril", "mayo", "junio",
  "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
];
// CENTRAL TIME, ALWAYS. The book is stamped for one parish and CT is the only clock that means
// anything to them; the machine's local zone is irrelevant and would silently vary. CDT/CST is
// resolved by the tz database via Intl, never computed by hand — a hand-rolled DST offset is a
// once-a-year wrong stamp on the page the whole choir opens. Labeled "CT" rather than CDT/CST so
// the line reads the same year-round.
const CT_TZ = "America/Chicago";
const centralNow = (dt) => {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: CT_TZ,
    year: "numeric", month: "numeric", day: "numeric",
    hour: "numeric", minute: "2-digit", hour12: true,
  }).formatToParts(dt);
  const get = (t) => (parts.find((p) => p.type === t) || {}).value;
  return {
    y: Number(get("year")), m: Number(get("month")), d: Number(get("day")),
    hour: Number(get("hour")), minute: get("minute"),
    ampm: String(get("dayPeriod") || "").toUpperCase().startsWith("A") ? "a.m." : "p.m.",
  };
};

const iso = arg("--date");
const isoTime = arg("--time");
let d;
if (iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso);
  if (!m) {
    console.error("--date must be YYYY-MM-DD");
    process.exit(2);
  }
  // --time is 24-hour Central, so a reproducible re-stamp needs no timezone reasoning.
  let hour24 = 0;
  let minute = "00";
  if (isoTime) {
    const t = /^(\d{1,2}):(\d{2})$/.exec(isoTime);
    if (!t || Number(t[1]) > 23 || Number(t[2]) > 59) {
      console.error("--time must be HH:MM, 24-hour, Central (e.g. 14:47)");
      process.exit(2);
    }
    hour24 = Number(t[1]);
    minute = t[2];
  }
  d = {
    y: Number(m[1]), m: Number(m[2]), d: Number(m[3]),
    hour: hour24 % 12 === 0 ? 12 : hour24 % 12,
    minute,
    ampm: hour24 < 12 ? "a.m." : "p.m.",
  };
} else {
  d = centralNow(new Date());
}
// Spanish long form: lowercase month, "de" on both sides, then the clock.
//   "3 de agosto de 2026, 2:47 p.m. CT · 371 páginas"
// The TIME is what makes two same-day editions distinguishable on the title page — the date alone
// cannot tell a morning book from the one that replaced it that afternoon, and the build badge
// answers a different question (the shell's number, not the book's).
const fecha = `${d.d} de ${MESES[d.m - 1]} de ${d.y}`;
const hora = `${d.hour}:${d.minute} ${d.ampm} CT`;
const label = `${fecha}, ${hora} \\267 ${pages} p\\341ginas`; // \267 = ·, \341 = á (ISOLatin1)
const human = `${fecha}, ${hora} · ${pages} páginas`;

// ── Build a one-page overlay at the exact page geometry ───────────────────────────────
// Small but readable: 14pt at the 115 DPI the book renders at is ~19px tall on device.
// Mid-grey so it reads as a colophon rather than competing with the music.
const FONT_SIZE = 14;
const MARGIN_X = 30;
const MARGIN_Y = 24;

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "sv-stamp-"));
const psPath = path.join(tmp, "stamp.ps");
const stampPdf = path.join(tmp, "stamp.pdf");

fs.writeFileSync(
  psPath,
  `%!PS-Adobe-3.0
<< /PageSize [${pageW} ${pageH}] >> setpagedevice
% Re-encode Helvetica to ISOLatin1 so the accented "páginas" and the "·" render.
/Helvetica findfont
dup length dict begin
  { 1 index /FID ne { def } { pop pop } ifelse } forall
  /Encoding ISOLatin1Encoding def
  currentdict
end
/Helvetica-SV exch definefont pop
/Helvetica-SV findfont ${FONT_SIZE} scalefont setfont
0.35 setgray
${MARGIN_X} ${MARGIN_Y} moveto
(${label}) show
showpage
`,
);

const gs = spawnSync(
  "gs",
  ["-q", "-dBATCH", "-dNOPAUSE", "-sDEVICE=pdfwrite",
    `-dDEVICEWIDTHPOINTS=${pageW}`, `-dDEVICEHEIGHTPOINTS=${pageH}`, "-dFIXEDMEDIA",
    `-sOutputFile=${stampPdf}`, psPath],
  { stdio: "inherit" },
);
if (gs.status !== 0) {
  console.error("ghostscript failed to build the stamp overlay.");
  process.exit(1);
}

// ── Overlay onto page 1 ONLY. qpdf composites; it does not re-encode the other pages. ──
const merged = path.join(tmp, "merged.pdf");
const overlay = spawnSync(
  "qpdf",
  [pdfPath, "--overlay", stampPdf, "--to=1", "--", merged],
  { stdio: "inherit" },
);
if (overlay.status !== 0) {
  console.error("qpdf overlay failed.");
  process.exit(1);
}

// ── Verify BEFORE overwriting: page count and geometry must be untouched. ─────────────
const after = spawnSync("pdfinfo", [merged], { encoding: "utf8" });
const pagesAfter = Number((after.stdout.match(/^Pages:\s+(\d+)/m) || [])[1]);
if (pagesAfter !== pages) {
  console.error(`❌ page count changed ${pages} → ${pagesAfter}; refusing to write.`);
  process.exit(1);
}
const sizeAfter = after.stdout.match(/^Page size:\s+([\d.]+) x ([\d.]+)/m);
if (!sizeAfter || Number(sizeAfter[1]) !== pageW || Number(sizeAfter[2]) !== pageH) {
  console.error(`❌ page geometry changed; refusing to write.`);
  process.exit(1);
}

fs.copyFileSync(merged, outPath);
fs.rmSync(tmp, { recursive: true, force: true });

console.log(`✅ stamped "${human}"`);
console.log(`   bottom-left of page 1 · ${pages} pages · ${pageW}x${pageH} pts unchanged`);
console.log(`   -> ${path.relative(root, outPath)}`);

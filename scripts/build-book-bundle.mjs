/**
 * Build an offline "book bundle" (page images + minimal indexes) from a PDF.
 *
 * This matches the runtime architecture in PdfReaderApp.tsx: the native app renders
 * pre-rendered page images, not a runtime PDF viewer.
 *
 * Usage (one-liners):
 *   node scripts/build-book-bundle.mjs --pdf assets/hymns-1.pdf --out assets/offline-books/hymns-1
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, "..");

const args = process.argv.slice(2);
const getArg = (name) => {
  const idx = args.indexOf(name);
  if (idx === -1) return null;
  return args[idx + 1] ?? null;
};

const pdfRel = getArg("--pdf");
const outRel = getArg("--out");
const reuse = args.includes("--reuse");
const dpi = Number(getArg("--dpi") ?? "144") || 144;
const thumbWidth = Number(getArg("--thumb-width") ?? "150") || 150;
const thumbQuality = Number(getArg("--thumb-quality") ?? "40") || 40;
if (!pdfRel || !outRel) {
  console.error("Missing required args: --pdf <path> --out <path>");
  process.exit(2);
}

const pdfPath = path.resolve(rootDir, pdfRel);
const outDir = path.resolve(rootDir, outRel);
const pagesDir = path.join(outDir, "pages");
const thumbsDir = path.join(outDir, "thumbs");

const rmrf = (p) => fs.rmSync(p, { recursive: true, force: true });
if (!reuse) {
  rmrf(outDir);
  fs.mkdirSync(pagesDir, { recursive: true });
  fs.mkdirSync(thumbsDir, { recursive: true });

  const pdftoppm = spawnSync(
    "pdftoppm",
    ["-jpeg", "-jpegopt", "quality=92", "-r", String(dpi), pdfPath, path.join(pagesDir, "page")],
    { stdio: "inherit" },
  );
  if (pdftoppm.status !== 0) {
    throw new Error(`pdftoppm failed with exit code ${pdftoppm.status ?? 1}`);
  }
}

const pageFiles = fs
  .readdirSync(pagesDir)
  .filter((f) => /^page-\d+\.jpg$/.test(f))
  .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

const totalPages = pageFiles.length;
if (!totalPages) throw new Error("No pages produced; expected page-###.jpg files.");

if (!reuse) {
  // Thumbs: 150px wide, quality 40, keep filenames aligned with pages.
  for (let i = 0; i < pageFiles.length; i++) {
    const file = pageFiles[i];
    const m = file.match(/^page-(\d+)\.jpg$/);
    const pageNum = Number(m[1]);
    const src = path.join(pagesDir, file);
    const out = path.join(thumbsDir, `thumb-${String(pageNum).padStart(3, "0")}.jpg`);
    const sips = spawnSync("sips", ["-Z", String(thumbWidth), "-s", "format", "jpeg", "-s", "formatOptions", String(thumbQuality), src, "--out", out], {
      stdio: "pipe",
    });
    if (sips.status !== 0 || !fs.existsSync(out)) {
      // Fallback: copy full-res if thumb generation fails; keep pipeline resilient.
      fs.copyFileSync(src, out);
    }
  }
}

// Extract text for per-page title/lyrics metadata (best-effort).
let rawAllText = "";
const pdftotext = spawnSync("pdftotext", ["-layout", "-enc", "UTF-8", pdfPath, "-"], { encoding: "utf8" });
if (pdftotext.status === 0) {
  rawAllText = pdftotext.stdout || "";
} else {
  console.warn(`pdftotext failed for ${path.basename(pdfPath)}; continuing without text index.`);
}

const pageTextsRaw = rawAllText.split("\f");

const normalizeText = (s) => {
  try {
    return s.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  } catch {
    return s.toLowerCase();
  }
};

const TITLE_RE_DOT = /^(\d+)\.\s+(.+)$/;
const TITLE_RE_SPACES = /^(\d+)\s{2,}(.+)$/;
const TITLE_RE_DASH = /^(\d+)\s*-\s*(.+)$/;
const CHORD_RE = /^[A-G][#b]?(?:m|M|maj|maj7|maj9|m7|m9|7|9|11|13|6|aug|dim|sus2|sus4|add9|add11)?(?:\/[A-G][#b]?)?$/;
const PERF_MARKER_RE = /^[\s(]*(?:coro|fin\b|final|intro\b|capo|codeta|puente|estrofa|verso|rep\.?|opcional|solista|solo\b|todos\b|hombres|mujeres|un[ií]sono|da\s+capo|d\.c\.|ritmo|balada|acorde|vuelta|voz\b|t\s*=|h\s*=|m\s*=|s\s*=)/i;

const extractTitleAndSong = (rawText) => {
  const lines = rawText.split("\n").map((l) => l.trim()).filter(Boolean);
  const scan = lines.slice(0, 12);
  for (const line of scan) {
    const m = line.match(TITLE_RE_DOT) || line.match(TITLE_RE_DASH) || line.match(TITLE_RE_SPACES);
    if (!m) continue;
    const song = Number(m[1]);
    if (!Number.isInteger(song) || song <= 0 || song > 5000) continue;
    const title = String(m[2] || "")
      .trim()
      .replace(/\s+Rev\s+\S+.*$/i, "")
      .replace(/\s+REV\s+\S+.*$/i, "")
      .replace(/\s+Rev\d+.*$/i, "")
      .replace(/\s+\d{2}\/\d{2}\/\d{4}.*$/, "")
      .replace(/\s+\d{8}.*$/, "")
      .replace(/[.]+\s*$/, "")
      .trim();
    if (!title) continue;
    return { song, title };
  }
  return null;
};

const extractLyrics = (songText) => {
  const lines = songText.split("\n").map((l) => l.trim()).filter(Boolean);
  const lyricLines = [];
  for (const line of lines) {
    if (/^\d+\.\s/.test(line)) continue;
    if (/^rev\s+\d/i.test(line)) continue;
    const tokens = line.split(/\s+/).filter(Boolean);
    if (!tokens.length) continue;
    const chordCount = tokens.filter((t) => CHORD_RE.test(t)).length;
    if (chordCount / tokens.length >= 0.6) continue;
    if (PERF_MARKER_RE.test(line)) continue;
    if (/^[\(\)\d\s.(2)]+$/.test(line)) continue;
    if (line.length < 5) continue;
    lyricLines.push(line);
    if (lyricLines.length >= 10) break;
  }
  return lyricLines.join(" • ");
};

const songIndex = [];
const songTitles = {};
const songSearchIndex = [];
const seenSong = new Set();

for (let pageIdx = 0; pageIdx < totalPages; pageIdx++) {
  const pageNum = pageIdx + 1;
  const text = pageTextsRaw[pageIdx] || "";
  const hit = extractTitleAndSong(text);
  if (hit && !seenSong.has(hit.song)) {
    const song = hit.song;
    const title = hit.title;
    seenSong.add(song);
    songIndex.push({ song, page: pageNum });
    songTitles[String(song)] = title;
    songSearchIndex.push({
      song,
      page: pageNum,
      title,
      normalized: normalizeText(`${song}. ${title}`),
      lyrics: extractLyrics(text),
    });
  }
}

songIndex.sort((a, b) => a.song - b.song);

const pagesJson = {
  totalPages,
  songIndex,
  themeIndex: [],
};

fs.writeFileSync(path.join(outDir, "pages.json"), JSON.stringify(pagesJson, null, 2));
fs.writeFileSync(path.join(outDir, "song-titles.json"), JSON.stringify(songTitles, null, 2));
fs.writeFileSync(path.join(outDir, "song-search-index.json"), JSON.stringify(songSearchIndex, null, 2));

console.log(`Built bundle: ${outRel}`);
console.log(`Pages: ${totalPages}, Songs indexed: ${songIndex.length}`);

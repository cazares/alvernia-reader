import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";

const rootDir = path.resolve(new URL("..", import.meta.url).pathname);
const srcDir = path.join(rootDir, "web", "src");
const distDir = path.join(rootDir, "web", "dist");
const booksDir = path.join(distDir, "books");

const parsePositiveInt = (value, fallback) => {
  const n = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
};

const parseJpegQuality = (value, fallback) => {
  const n = Number.parseInt(String(value ?? "").trim(), 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.min(100, n));
};

// Rendering pages from the PDF is the single biggest lever for size vs sharpness.
// Phone-readable target: render lossless PNG, then encode compact WebP
// (pdftoppm can't emit WebP directly). ~115 DPI + q60 ≈ ~28MB for the full manual.
// (Defined up here, before the version stamps, because they feed bookVersion below.)
const PDF_RENDER_DPI = parsePositiveInt(process.env.ALVERNIA_PDF_RENDER_DPI, 115);
const WEBP_QUALITY = parseJpegQuality(process.env.ALVERNIA_PDF_WEBP_QUALITY, 60);
// FROZEN, deliberately not an env knob and not derived from the page count. See the long note at
// the rename site in renderPagesToWebp: this is the width that keeps page-001.webp meaning page 1
// forever, across every future book size. web/src/app.js declares the same constant. Changing it
// is a full-book rename that invalidates every offline copy in the field simultaneously.
const PAGE_PAD_WIDTH = 3;
// STABLE NAME ON PURPOSE. This used to be signo_vivo_<pageCount>.pdf, which meant every book
// update should have renamed the file and chased five references — so nobody did, and the file sat
// named "_372" while containing 373 pages for a day, misleading everyone who read it. The page
// count already lives in three authoritative places (pdfinfo, the bundle manifest, the title
// stamp); a fourth copy encoded in a filename is just one that goes stale.
const BOOK_PDF_PATH = path.join(rootDir, "assets", "songbook.pdf");

// Content-address of the BOOK: the source PDF's bytes plus the two render knobs that
// determine the output pixels. This is what keys the page-image cache (PAGE_CACHE in
// sw.js/app.js), SEPARATELY from the shell's cacheVersion, so that:
//   - a shell-only deploy does NOT bust the page cache (no 25MB re-download for a CSS fix);
//   - ANY change to the book — including a page revised IN PLACE under an unchanged
//     page-NNN.webp filename — busts it, and the SW's network-first page path (sw.js)
//     then fetches the new bytes instead of resurrecting the old ones.
// Before this existed, cacheVersion hashed only shell files, so an in-place page edit
// (the build-377 header cleanup class, or the title-page date stamp) NEVER reached any
// device that already had the page cached. Appending pages worked; correcting one didn't.
const bookVersion = (() => {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(BOOK_PDF_PATH));
  hash.update(`dpi=${PDF_RENDER_DPI};q=${WEBP_QUALITY}`);
  return hash.digest("hex").slice(0, 12);
})();

const cacheVersion = (() => {
  const gitSha = spawnSync("git", ["rev-parse", "--short", "HEAD"], {
    cwd: rootDir,
    encoding: "utf8",
  });
  const sha = gitSha.status === 0 && gitSha.stdout.trim() ? gitSha.stdout.trim() : "nogit";
  // Content hash of every source file that determines the built bundle. CRITICAL: the bundle is
  // built from the WORKING TREE, so a git-SHA-only version COLLIDES across two deploys made at the
  // same HEAD with different uncommitted content — returning users would keep a stale Service Worker
  // cache and never receive the fix. Hashing the actual source guarantees the cache busts whenever
  // the content changes, regardless of commit state. The SHA prefix stays for human traceability.
  const hash = crypto.createHash("sha256");
  for (const f of ["app.js", "sw.js", "styles.css", "index.html", "manifest.webmanifest"]) {
    const p = path.join(srcDir, f);
    if (fs.existsSync(p)) hash.update(fs.readFileSync(p));
  }
  hash.update(fs.readFileSync(new URL(import.meta.url))); // build.mjs itself (template/inlining logic)
  return `${sha}-${hash.digest("hex").slice(0, 8)}`;
})();
// Human-facing build number (from version.json) baked into the bundle so signovivo.com can show
// "v<NNN>" — on native the shell injects __SIGNO_VINO_NATIVE_BUNDLE_VERSION instead (and shows its
// own overlay). Lets anyone confirm at a glance which build a device/tab is on.
const buildNumber = (() => {
  try {
    const v = JSON.parse(fs.readFileSync(path.join(rootDir, "version.json"), "utf8"));
    return String(v.buildNumber ?? "");
  } catch {
    return "";
  }
})();
/** Marketing version ("1.0.4"), so the badge can read `1.0.4 · 395b · 396w · 374p`. */
const baseVersion = (() => {
  try {
    const v = JSON.parse(fs.readFileSync(path.join(rootDir, "version.json"), "utf8"));
    return String(v.baseVersion ?? "");
  } catch {
    return "";
  }
})();

fs.rmSync(distDir, { recursive: true, force: true });
fs.mkdirSync(distDir, { recursive: true });
fs.mkdirSync(booksDir, { recursive: true });

fs.copyFileSync(path.join(srcDir, "styles.css"), path.join(distDir, "styles.css"));

// Copy web/src/lib/*.js verbatim into dist/lib/. These are tiny, dependency-free
// helpers loaded as <script defer> BEFORE app.js (e.g. svRelayRoom, the relay-room
// resolver for the staging/canary channel). Kept as separate files so they are
// unit-testable in node and, if one ever fails to load, app.js's own guards still
// fall back to safe defaults.
const libSrcDir = path.join(srcDir, "lib");
if (fs.existsSync(libSrcDir)) {
  const libOutDir = path.join(distDir, "lib");
  fs.mkdirSync(libOutDir, { recursive: true });
  for (const libFile of fs.readdirSync(libSrcDir)) {
    if (libFile.endsWith(".js")) {
      fs.copyFileSync(path.join(libSrcDir, libFile), path.join(libOutDir, libFile));
    }
  }
}

const relayBase = (process.env.ALVERNIA_RELAY_BASE || "https://signovivo-sync.4j4982y8jp.workers.dev").replace(/\/+$/, "");
// app.js is WRITTEN LATER (just before index.html), not here: one of its tokens is the shipped
// page count, which is not known until the book has actually been rendered. Nothing between here
// and there reads dist/app.js, and cacheVersion hashes web/src/app.js (the source), not the
// emitted file, so deferring the write changes no other output.
const emitAppJs = (totalPages) => {
  const appSource = fs
    .readFileSync(path.join(srcDir, "app.js"), "utf8")
    .replaceAll("__CACHE_VERSION__", cacheVersion)
    .replaceAll("__BOOK_VERSION__", bookVersion)
    .replaceAll("__RELAY_BASE__", relayBase)
    .replaceAll("__BUILD_NUMBER__", buildNumber)
    .replaceAll("__BASE_VERSION__", baseVersion)
    .replaceAll("__STANDARD_TOTAL_PAGES__", String(totalPages));
  fs.writeFileSync(path.join(distDir, "app.js"), appSource);
};
fs.copyFileSync(path.join(srcDir, "manifest.webmanifest"), path.join(distDir, "manifest.webmanifest"));
// app.js and index.html are written later, once the page count exists

// Icons are pre-optimized committed assets (pngquant + oxipng), NOT generated at
// build time: sips emits unoptimized truecolor PNGs, and the SW install precache
// re-downloads all three icons on every deploy on every device (~1.5 MB → ~0.6 MB).
// The pristine 1024px master stays at assets/icon.png; if the artwork ever changes,
// regenerate these with scripts/regen-web-icons.sh.
for (const iconName of ["icon.png", "icon-192.png", "icon-512.png"]) {
  fs.copyFileSync(path.join(srcDir, iconName), path.join(distDir, iconName));
}
const serviceWorkerSource = fs
  .readFileSync(path.join(srcDir, "sw.js"), "utf8")
  .replaceAll("__CACHE_VERSION__", cacheVersion)
  // NOTE: a book-only change leaves cacheVersion untouched but still changes the EMITTED
  // sw.js bytes (this token) — which is exactly what triggers the browser's SW update flow
  // and rolls the fleet onto the new book. The propagation ride-along is load-bearing.
  .replaceAll("__BOOK_VERSION__", bookVersion);
fs.writeFileSync(path.join(distDir, "sw.js"), serviceWorkerSource);

// ─── Shared page render + encode ─────────────────────────────────────────────
// (PDF_RENDER_DPI / WEBP_QUALITY are defined at the top of the file — they feed bookVersion.)
// Renders every PDF page to WebP under <bookOutDir>/pages/page-NNN.webp using the
// same pdftoppm→cwebp path for every book. Returns the sorted list of webp files.
const renderPagesToWebp = (pdfPath, pagesOutDir) => {
  fs.mkdirSync(pagesOutDir, { recursive: true });
  const pngPrefix = path.join(pagesOutDir, "render");
  const render = spawnSync(
    "pdftoppm",
    ["-png", "-r", String(PDF_RENDER_DPI), pdfPath, pngPrefix],
    { stdio: "inherit" },
  );

  if (render.status !== 0) {
    throw new Error(`pdftoppm failed with exit code ${render.status ?? 1} for ${pdfPath}`);
  }

  // Sort by page NUMBER, never lexically. pdftoppm's own pad width tracks the page count, so a
  // book that crosses 1000 emits render-0001..render-1000 and a string sort puts render-1000
  // before render-0999 — which would silently mis-map the search index below (it pairs
  // pageFiles[idx] with page idx+1). Numeric sort is correct at every book size.
  const pageNo = (file) => Number(file.match(/-(\d+)\.(?:png|webp)$/)[1]);
  const pngFiles = fs
    .readdirSync(pagesOutDir)
    .filter((file) => /^render-\d+\.png$/.test(file))
    .sort((left, right) => pageNo(left) - pageNo(right));

  // Encode each rendered page to WebP (page-NNN.webp), then drop the intermediate PNG.
  console.log(`Encoding ${pngFiles.length} pages to WebP q${WEBP_QUALITY} @ ${PDF_RENDER_DPI} DPI...`);
  for (let i = 0; i < pngFiles.length; i++) {
    // FROZEN pad width — do NOT derive this from the page count. pdftoppm pads to the width of the
    // total, so at 1000 pages it would emit page-0001.webp and RENAME every existing page URL. Every
    // offline copy the additive-only invariant protects — cached page images, previous SW caches, a
    // staged download — is keyed by that filename, so a book crossing 1000 would invalidate the
    // entire installed base at once, silently. Padding to a FIXED minimum of 3 keeps page-001..999
    // permanent forever and lets page 1000+ simply be longer, which is genuinely additive.
    // web/src/app.js's PAGE_PAD_WIDTH is the reader half of this contract; smoke-boot pins both.
    const num = String(pageNo(pngFiles[i])).padStart(PAGE_PAD_WIDTH, "0");
    const pngPath = path.join(pagesOutDir, pngFiles[i]);
    const webpOut = path.join(pagesOutDir, `page-${num}.webp`);
    const webp = spawnSync(
      "cwebp",
      ["-quiet", "-q", String(WEBP_QUALITY), pngPath, "-o", webpOut],
      { stdio: "pipe" },
    );
    if (webp.status !== 0 || !fs.existsSync(webpOut)) {
      throw new Error(`cwebp failed for ${pngFiles[i]} (status ${webp.status ?? "?"})`);
    }
    fs.unlinkSync(pngPath);
    if ((i + 1) % 50 === 0) process.stdout.write(`  ${i + 1}/${pngFiles.length}\n`);
  }
  console.log("WebP generation done.");

  return fs
    .readdirSync(pagesOutDir)
    .filter((file) => /^page-\d+\.webp$/.test(file))
    .sort((left, right) => pageNo(left) - pageNo(right));
};

// ─── Song Title Overrides ────────────────────────────────────────────────────
// Manual corrections for songs where OCR title extraction is inaccurate

const TITLE_OVERRIDES = {
  24:  "Santo Juvenil",
  61:  "Vía Crucis",
  74:  "3ª Estación — Vía Crucis",
  115: "13ª Estación — Vía Crucis",
  198: "Señor quien puede entrar",
  249: "Solamente de la Santa Trinidad",
  371: "Santo Español",
};

// ─── Song Title Extraction ───────────────────────────────────────────────────
// Titles are always on the first non-empty line in format "N. Title [Rev ...]"

const extractTitle = (rawText) => {
  const lines = rawText.split("\n").map((l) => l.trim()).filter(Boolean);
  if (!lines[0]) return null;
  // Match "N. Title" — strip trailing revision metadata
  const m = lines[0].match(/^\d+\.\s+(.+)/);
  if (!m) return null;
  return m[1]
    .replace(/\s+Rev\s+\S+.*$/i, "")
    .replace(/\s+REV\s+\S+.*$/i, "")
    .replace(/\s+Rev\d+.*$/i, "")
    .replace(/\s+\d{2}\/\d{2}\/\d{4}.*$/, "")
    .replace(/\s+\d{8}.*$/, "")
    .replace(/[.]+\s*$/, "")
    .trim();
};

// ─── Lyric Extraction ────────────────────────────────────────────────────────
// Strip chord-only lines and performance markers; return first ~10 lyric lines
// joined as a single searchable string.

const PERF_MARKER_RE = /^[\s(]*(?:coro|fin\b|final|intro\b|capo|codeta|puente|estrofa|verso|rep\.?|opcional|solista|solo\b|todos\b|hombres|mujeres|un[ií]sono|da\s+capo|d\.c\.|ritmo|balada|acorde|vuelta|voz\b|t\s*=|h\s*=|m\s*=|s\s*=)/i;

const extractLyrics = (songText) => {
  const lines = songText.split("\n").map((l) => l.trim()).filter(Boolean);
  const lyricLines = [];
  for (const line of lines) {
    if (/^\d+\.\s/.test(line)) continue;                          // title line
    if (/^rev\s+\d/i.test(line)) continue;                        // revision tag
    const tokens = line.split(/\s+/).filter(Boolean);
    if (tokens.length === 0) continue;
    const chordCount = tokens.filter((t) => CHORD_RE.test(t)).length;
    if (chordCount / tokens.length >= 0.6) continue;              // chord-only line
    if (PERF_MARKER_RE.test(line)) continue;                      // performance marker
    if (/^[\(\)\d\s.(2)]+$/.test(line)) continue;                 // parenthetical / repeat markers
    if (line.length < 5) continue;                                // too short
    lyricLines.push(line);
    if (lyricLines.length >= 10) break;
  }
  return lyricLines.join(" • ");
};

// ─── Music Theory: Solfège + Key Detection ───────────────────────────────────

const SOLFEGE = {
  C: "Do", D: "Re", E: "Mi", F: "Fa", G: "Sol", A: "La", B: "Si",
  "C#": "Do sostenido", "D#": "Re sostenido", "F#": "Fa sostenido",
  "G#": "Sol sostenido", "A#": "La sostenido",
  Db: "Re bemol", Eb: "Mi bemol", Gb: "Sol bemol",
  Ab: "La bemol", Bb: "Si bemol",
};

const ALL_ROOTS = ["C", "Db", "D", "Eb", "E", "F", "F#", "G", "Ab", "A", "Bb", "B"];

// Static major-scale note names for each of the 12 ALL_ROOTS — sharps for
// C/D/E/G/A/B majors, flats for Db/Eb/F/Ab/Bb majors. Hardcoded so the build has
// no music-theory dependency.
const MAJOR_SCALES = {
  C:  ["C", "D", "E", "F", "G", "A", "B"],
  Db: ["Db", "Eb", "F", "Gb", "Ab", "Bb", "C"],
  D:  ["D", "E", "F#", "G", "A", "B", "C#"],
  Eb: ["Eb", "F", "G", "Ab", "Bb", "C", "D"],
  E:  ["E", "F#", "G#", "A", "B", "C#", "D#"],
  F:  ["F", "G", "A", "Bb", "C", "D", "E"],
  "F#": ["F#", "G#", "A#", "B", "C#", "D#", "E#"],
  G:  ["G", "A", "B", "C", "D", "E", "F#"],
  Ab: ["Ab", "Bb", "C", "Db", "Eb", "F", "G"],
  A:  ["A", "B", "C#", "D", "E", "F#", "G#"],
  Bb: ["Bb", "C", "D", "Eb", "F", "G", "A"],
  B:  ["B", "C#", "D#", "E", "F#", "G#", "A#"],
};

// Enharmonic normalization for single-accidental note names ([A-G][#b]?). Chord
// roots come from `extractChordRoot`, which only ever yields [A-G][#b]?, so
// single-accidental coverage is sufficient. Only E#→F, B#→C, Cb→B, Fb→E actually
// move; everything else maps to itself.
const SIMPLIFY_NOTE = {
  A: "A", B: "B", C: "C", D: "D", E: "E", F: "F", G: "G",
  "A#": "A#", "C#": "C#", "D#": "D#", "F#": "F#", "G#": "G#",
  Ab: "Ab", Bb: "Bb", Db: "Db", Eb: "Eb", Gb: "Gb",
  "B#": "C", "E#": "F", Cb: "B", Fb: "E",
};

const simplifyNote = (n) => SIMPLIFY_NOTE[n] || n;

// Single chord regex — matches common guitar chord names
const CHORD_RE = /^[A-G][#b]?(?:m|M|maj|maj7|maj9|m7|m9|7|9|11|13|6|aug|dim|sus2|sus4|add9|add11)?(?:\/[A-G][#b]?)?$/;

const extractChordRoot = (chord) => {
  // Strip slash bass note
  const base = chord.split("/")[0];
  const m = base.match(/^([A-G][#b]?)/);
  return m ? m[1] : null;
};

// Score how well a set of chord roots fits a given major key
const scoreRootsInKey = (roots, tonic) => {
  const majorNotes = new Set((MAJOR_SCALES[tonic] || []).map((n) => simplifyNote(n)));
  let score = 0;
  for (const r of roots) {
    const simplified = simplifyNote(r);
    if (majorNotes.has(simplified)) score += 1;
  }
  // Small bonus when the first chord's root matches the tonic (home-key feel)
  const firstSimplified = simplifyNote(roots[0]);
  if (firstSimplified === tonic) score += 0.4;
  return score;
};

// Detect most likely major key root from a list of chord name strings
const detectKeyFromChords = (chords) => {
  if (!chords || chords.length === 0) return null;
  const roots = chords.map(extractChordRoot).filter(Boolean);
  if (!roots.length) return null;

  let bestKey = null;
  let bestScore = -1;
  for (const tonic of ALL_ROOTS) {
    const score = scoreRootsInKey(roots, tonic);
    if (score > bestScore) { bestScore = score; bestKey = tonic; }
  }
  return bestKey;
};

// Extract all chords from OCR text using chord-line heuristic
const extractChordsFromText = (text) => {
  const allChords = [];
  for (const line of text.split("\n")) {
    const tokens = line.trim().split(/\s+/).filter(Boolean);
    if (tokens.length < 2) continue;
    const chords = tokens.filter((t) => CHORD_RE.test(t));
    if (chords.length >= 2 && chords.length / tokens.length >= 0.55) {
      allChords.push(...chords);
    }
  }
  return allChords;
};

// Parse "Intro: G Em D C" line and optional capo from song text
const parseIntroInfo = (songText) => {
  const lines = songText.split("\n");
  let introChords = null;
  let capo = null;

  for (const line of lines) {
    const trimmed = line.trim();

    // Detect "Intro:" line (various spellings)
    if (!introChords && /^intro(?:ducción|duccion|\.)?[\s:.]*/i.test(trimmed)) {
      const afterIntro = trimmed.replace(/^intro(?:ducción|duccion|\.)?[\s:.]*/i, "");
      const tokens = afterIntro.split(/[\s,\-|/]+/).filter(Boolean);
      const chords = tokens.filter((t) => CHORD_RE.test(t));
      if (chords.length >= 2) {
        introChords = chords;
      }
    }

    // Detect capo indication: "Capo 2", "Cejilla 3", "cejilla en 2"
    if (!capo) {
      const capoMatch = trimmed.match(/(?:capo|cejilla)\s*(?:en\s+)?(\d+)/i);
      if (capoMatch) capo = Number.parseInt(capoMatch[1], 10);
    }
  }

  if (!introChords) return null;

  const key = detectKeyFromChords(introChords);
  if (!key) return null;

  const solfege = SOLFEGE[key] || key;
  return { key, solfege, chords: introChords, capo: capo || null };
};

// Harmonic complexity: simple (≤4 roots), medio (5–7), avanzado (8+/complex)
const scoreComplexity = (allChords) => {
  if (!allChords || allChords.length === 0) return "simple";
  const uniqueRoots = new Set(allChords.map(extractChordRoot).filter(Boolean));
  const complexCount = allChords.filter((c) => /(?:dim|aug|maj7|m7b5|7b9|b5|#5|#11|b13)/.test(c)).length;
  const seventhCount = allChords.filter((c) => /(?:^|[a-z])(?:m7|7|9|11|13)(?:$|[^0-9])/.test(c)).length;
  const slashCount = allChords.filter((c) => c.includes("/")).length;
  let score = uniqueRoots.size;
  if (complexCount >= 1) score += 2;
  if (seventhCount >= 3) score += 1;
  if (slashCount >= 2) score += 1;
  if (score <= 4) return "simple";
  if (score <= 7) return "medio";
  return "avanzado";
};

// ─── Theme Taxonomy ──────────────────────────────────────────────────────────

const THEMES = [
  {
    id: "cuaresma",
    label: "Cuaresma / Semana Santa",
    emoji: "✝️",
    phrases: ["via crucis", "viernes santo", "semana santa", "saliendo del pretorio", "corona de espinas", "cargando la cruz"],
    words: ["cruz", "pasión", "calvario", "crucifixión", "pretorio", "sufrimiento", "heridas", "espinas", "clavo", "crucificado", "procesión", "estación", "gólgota", "madero", "nazareno", "cuaresma", "cenizas", "ceniza", "ayuno", "penitencia", "lanzada", "flagelado", "atado", "golpeado"],
  },
  {
    id: "resurreccion",
    label: "Resurrección / Pascua",
    emoji: "🌅",
    phrases: ["resucitó", "ha vencido", "tercer día", "muerte fue vencida", "aleluya el señor resucitó", "el señor resucitó", "el señor resusito"],
    words: ["resurrección", "pascua", "vive", "resucitado", "vencedor", "tumba", "sepulcro", "ascensión", "ascensión"],
  },
  {
    id: "espiritu_santo",
    label: "Espíritu Santo",
    emoji: "🕊️",
    phrases: ["espíritu santo", "espíritu de dios", "espiritu santo", "espiritu de dios", "ven espíritu", "día de pentecostés", "dios de pentecostés"],
    words: ["paráclito", "pentecostés", "unción", "inúndame", "desciende", "muévete", "soplo", "fuego"],
  },
  {
    id: "eucaristia",
    label: "Eucaristía",
    emoji: "🍞",
    phrases: ["pan de vida", "pan del cielo", "cuerpo de cristo", "cordero de dios", "pan y vino", "bendita eucaristía", "jesus eucaristia", "jesús eucaristía", "presencia real", "pan de vida eterna", "sobre tu altar"],
    words: ["eucaristía", "eucaristia", "comunión", "altar", "ofertorio", "ofrendas"],
  },
  {
    id: "misa",
    label: "Partes de la Misa",
    emoji: "⛪",
    phrases: ["señor señor piedad", "oh señor ten piedad", "cordero de dios", "gloria a dios", "ten piedad"],
    words: ["kyrie", "sanctus", "piedad", "gloria", "aleluya", "santo"],
  },
  {
    id: "maria",
    label: "Virgen María",
    emoji: "💙",
    phrases: ["dios te salve maría", "dios te salve maria", "ave maría", "ave maria", "contigo maría", "junto a ti maría", "junto a ti maria", "señora señora", "madre eres", "madre maria", "niña de mis ojos", "de nazaret", "maría de", "la virgen"],
    words: ["maría", "virgen", "guadalupe", "inmaculada", "reina", "nazaret", "maria", "señora"],
  },
  {
    id: "alabanza",
    label: "Alabanza / Adoración",
    emoji: "🙌",
    phrases: ["cuán grande", "gloria al rey", "honor y gloria", "te adoraré", "adorándote", "yo quiero alabar"],
    words: ["alabanza", "adoración", "adoracion", "hosanna", "magnificar", "exaltado", "glorioso", "majestad", "incomparable", "incomparable"],
  },
  {
    id: "sanacion",
    label: "Sanación / Perdón",
    emoji: "🙏",
    phrases: ["sáname señor", "saname señor", "perdona tu pueblo", "toma mis lágrimas", "renuévame señor"],
    words: ["sanar", "sáneme", "perdón", "misericordia", "restaurar", "renuévame", "arrepentimiento", "herido", "curar", "reconciliación"],
  },
  {
    id: "comunidad",
    label: "Comunidad / Alvernia",
    emoji: "🤝",
    phrases: ["monte alvernia", "himno alvernia", "experiencia alvernia", "juntos como hermanos"],
    words: ["alvernia", "hermanos", "comunidad", "cofrades", "movimiento", "unidos"],
  },
  {
    id: "navidad",
    label: "Navidad",
    emoji: "🎄",
    phrases: ["campana sobre campana", "burrito sabanero", "niño jesús", "noche buena", "nochebuena", "reyes magos", "la sagrada familia"],
    words: ["navidad", "nacimiento", "belén", "belen", "pastores", "pesebre", "tamborilero", "villancico", "navideño", "posada"],
  },
  {
    id: "mision",
    label: "Misión / Vocación",
    emoji: "🌍",
    phrases: ["alma misionera", "pescador de hombres", "peregrino a donde vas", "tú me llamaste"],
    words: ["misión", "misionero", "evangelio", "proclamar", "testigo", "vocación"],
  },
  {
    id: "fe",
    label: "Fe / Esperanza",
    emoji: "⭐",
    phrases: ["granito de mostaza", "con un paso de fe", "yo creo en ti", "yo creo en las promesas"],
    words: ["confianza", "confiar", "promesas", "esperanza", "certeza", "firmeza"],
  },
  {
    id: "ninos",
    label: "Canciones para Niños",
    emoji: "🐸",
    phrases: ["el sapo", "el tren", "el baile de la mane", "el africanito", "patos pollos y gallinas", "como alaba el pato", "tienes que ser un niño", "las alas de la mariposa"],
    words: [],
  },
  // ── Liturgical seasons ──────────────────────────────────────────────────────
  // NOTE: adviento removed — no songs in the songbook matched its phrase/word set.
  // Keeping it would produce a dead filter in the Temas tab (the UI already hides empty
  // themes, but a zero-song entry in themeIndex is misleading data).
  // ── Sacramental / life-event themes ────────────────────────────────────────
  {
    id: "bautismo",
    label: "Bautismo / Iniciación",
    emoji: "💧",
    phrases: ["agua y espíritu", "bautizados en cristo", "lavaste mis pecados", "renacido en el agua", "en el nombre del padre", "agua viva", "fuente de vida"],
    words: ["bautismo", "bautizar", "bautizado", "pila", "iniciación", "catecúmeno", "catecúmenos", "neófito"],
  },
  {
    id: "bodas",
    label: "Bodas / Matrimonio",
    emoji: "💍",
    phrases: ["amor conyugal", "cuando dos se aman", "el amor es paciente", "el amor es benigno", "el amor todo lo puede", "en el día de tu boda", "dos en uno"],
    words: ["boda", "bodas", "matrimonio", "esposos", "novios", "alianza matrimonial", "casamiento", "desposorio", "esposo", "esposa"],
  },
  // funerales removed — no songs matched. See adviento note above.
  {
    id: "confirmacion",
    label: "Confirmación",
    emoji: "🔥",
    phrases: ["sellados por el espíritu", "confirmados en la fe", "ungidos por el espíritu", "vengan al altar de dios"],
    words: ["confirmación", "confirmados", "crismación", "unción", "ungido", "sello"],
  },
  // primera_comunion removed — no songs matched. See adviento note above.
  // ── Functional / directorial themes ────────────────────────────────────────
  {
    id: "entrada",
    label: "Entrada / Inicio de Misa",
    emoji: "🚪",
    phrases: ["entremos en la casa del señor", "venid al templo", "venimos a adorarte", "hoy venimos ante ti", "entramos en tu presencia", "aquí estamos reunidos"],
    words: ["entramos", "venimos", "congregamos", "reunidos", "asamblea", "gathering"],
  },
  // envio removed — no songs matched. See adviento note above.
  {
    id: "paz",
    label: "Paz / Unidad",
    emoji: "☮️",
    phrases: ["la paz sea con vosotros", "daos la paz", "paz en la tierra", "danos tu paz", "hijos de la paz", "sembradores de paz", "paz a este mundo"],
    words: ["paz", "armonía", "unidad", "concordia", "fraternidad", "reconciliar", "pacificador"],
  },
  {
    id: "procesion",
    label: "Procesión / Peregrinación",
    emoji: "🚶",
    phrases: ["en procesión", "camino al altar", "somos peregrinos", "juntos en el camino", "caminamos hacia ti"],
    words: ["procesión", "peregrinación", "peregrinos", "marcha", "caminamos", "peregrinar"],
  },
  // santos removed — no songs matched. See adviento note above.
];

// Normalize: NFD + strip accents + lowercase
const normalize = (text) =>
  text.normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase();

const scoreThemes = (rawText) => {
  if (!rawText) return [];

  const lines = rawText.split("\n").map((l) => l.trim()).filter(Boolean);
  const title = (lines[0] || "").replace(/^\d+\.\s*/, "");
  const top3Lines = lines.slice(1, 4).join(" ");
  const rest = lines.slice(4).join(" ");

  const normTitle = normalize(title);
  const normTop = normalize(top3Lines);
  const normRest = normalize(rest);

  const scores = THEMES.map((theme) => {
    let score = 0;

    for (const phrase of theme.phrases) {
      const p = normalize(phrase);
      const phraseWeight = 1 + Math.floor(phrase.split(" ").length / 2); // longer phrase = higher weight
      if (normTitle.includes(p)) score += 8 * phraseWeight;
      else if (normTop.includes(p)) score += 4 * phraseWeight;
      else if (normRest.includes(p)) score += 2 * phraseWeight;
    }

    for (const word of theme.words) {
      const w = normalize(word);
      const titleCount = (normTitle.match(new RegExp(`\\b${w}\\b`, "g")) || []).length;
      const topCount = (normTop.match(new RegExp(`\\b${w}\\b`, "g")) || []).length;
      const restCount = (normRest.match(new RegExp(`\\b${w}\\b`, "g")) || []).length;
      score += titleCount * 5 + topCount * 2 + restCount * 1;
    }

    return { id: theme.id, score };
  });

  // Return top themes above threshold, sorted by score desc, max 3
  return scores
    .filter((s) => s.score >= 4)
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map((s) => s.id);
};

const themeIndex = THEMES.map(({ id, label, emoji }) => ({ id, label, emoji }));

// ─── Per-book builders ───────────────────────────────────────────────────────

// Full OCR pipeline for the standard manual: parse songIndex from source, run
// pdftotext, derive titles/themes/keys/intro/complexity, and emit the four
// per-book manifests. Mirrors the original single-book build exactly.
const buildStandardManifests = ({ pageFiles, pdfPath, bookOutDir }) => {
  // A SONG NUMBER IS A PAGE NUMBER. Owner's call, 2026-08-05: "the reader should associate with
  // PAGES not song numbers." This used to be parsed out of a hand-maintained [song, page] list in
  // src/alverniaManual2SongIndex.js, which had to be edited by hand for every new book — the single
  // thing standing between "here is a PDF" and a published songbook. It is now derived: page n is
  // song n, for however many pages the PDF has. Nothing to maintain, and any PDF works.
  //
  // The shape is unchanged on purpose, so app.js and the native reader keep consuming it as they
  // always have. What changed is only that song and page are now the same number.
  const songIndex = pageFiles.map((_, i) => ({ song: i + 1, page: i + 1 }));

  // ─── PDF Text Extraction ──────────────────────────────────────────────────

  const pdfTextResult = spawnSync(
    "pdftotext",
    ["-layout", "-enc", "UTF-8", pdfPath, "-"],
    { encoding: "utf8" },
  );

  if (pdfTextResult.status !== 0) {
    throw new Error(`pdftotext failed with exit code ${pdfTextResult.status ?? 1}`);
  }

  const rawAllText = pdfTextResult.stdout || "";
  const pageTextsRaw = rawAllText.split("\f");

  // ─── Enrich songIndex with titles + themes + keys + intro ─────────────────

  const songSearchIndex = {};

  for (let i = 0; i < songIndex.length; i += 1) {
    const entry = songIndex[i];
    const nextEntry = songIndex[i + 1] || null;
    const endPage = nextEntry ? nextEntry.page - 1 : pageTextsRaw.length;

    // Collect all OCR text pages for this song
    const songPages = pageTextsRaw.slice(entry.page - 1, endPage);
    const songText = songPages.join("\n");
    const firstPageText = pageTextsRaw[entry.page - 1] || "";

    entry.title = TITLE_OVERRIDES[entry.song] || extractTitle(firstPageText) || null;
    entry.themes = scoreThemes(firstPageText);
    entry.lyrics = extractLyrics(songPages.join("\n"));
    // For native in-app search we keep the full OCR blob per song.
    songSearchIndex[String(entry.song)] = songText;

    // Detect key from all chord lines in the song
    const allChords = extractChordsFromText(songText);
    const detectedKey = detectKeyFromChords(allChords);
    entry.key = detectedKey || null;
    entry.solfege = detectedKey ? (SOLFEGE[detectedKey] || detectedKey) : null;

    // Harmonic complexity
    entry.complexity = scoreComplexity(allChords);

    // Parse intro info from first page
    entry.intro = parseIntroInfo(firstPageText);
  }

  // ─── Search Index ──────────────────────────────────────────────────────────

  const searchIndexPages = pageFiles.map((file, idx) => {
    const pageNum = idx + 1;
    const songEntry = songIndex.find((s) => s.page === pageNum);
    let text;
    if (songEntry) {
      // Song pages: index title + clean lyrics (no chord names) for accurate lyric search
      const parts = [songEntry.title || "", songEntry.lyrics || ""].filter(Boolean);
      text = parts.join(" ").replace(/\s+/g, " ").trim().slice(0, 800);
    } else {
      text = (pageTextsRaw[idx] || "").replace(/\s+/g, " ").trim().slice(0, 600);
    }
    return { page: pageNum, text };
  }).filter((entry) => entry.text.length > 5);

  fs.writeFileSync(
    path.join(bookOutDir, "search-index.json"),
    JSON.stringify({ pages: searchIndexPages }),
  );

  fs.writeFileSync(
    path.join(bookOutDir, "pages.json"),
    JSON.stringify({ totalPages: pageFiles.length, songIndex, themeIndex }),
  );

  // Used by the native reader for fast title lookups without pulling the full songIndex.
  const songTitles = {};
  for (const entry of songIndex) {
    if (entry?.song && entry?.title) songTitles[String(entry.song)] = entry.title;
  }
  fs.writeFileSync(path.join(bookOutDir, "song-titles.json"), JSON.stringify(songTitles));
  fs.writeFileSync(path.join(bookOutDir, "song-search-index.json"), JSON.stringify(songSearchIndex));

  return pageFiles.length;
};

// Render the book's pages and emit its manifests via the full OCR pipeline.
const buildBook = ({ id, label, pdfPath }) => {
  console.log(`\n── Building book "${id}" (${label}) ──`);
  const bookOutDir = path.join(booksDir, id);
  fs.mkdirSync(bookOutDir, { recursive: true });
  const pagesOutDir = path.join(bookOutDir, "pages");

  const pageFiles = renderPagesToWebp(pdfPath, pagesOutDir);
  const totalPages = buildStandardManifests({ pageFiles, pdfPath, bookOutDir });

  console.log(`Book "${id}" done — ${totalPages} pages.`);
  return { id, label, totalPages };
};

// ─── Build the single book ───────────────────────────────────────────────────

const DEFAULT_BOOK = "standard";

const standard = buildBook({
  id: DEFAULT_BOOK,
  label: "Manual Alvernia",
  pdfPath: BOOK_PDF_PATH, // single source of truth — the same path bookVersion hashes
});

// ─── books.json — single-book registry ───────────────────────────────────────

const booksManifest = {
  default: DEFAULT_BOOK,
  books: {
    [standard.id]: { label: standard.label, totalPages: standard.totalPages },
  },
};
fs.writeFileSync(path.join(distDir, "books.json"), JSON.stringify(booksManifest));

const defaultTotalPages = booksManifest.books[DEFAULT_BOOK].totalPages;

// ─── Shell HTML with inlined book registry + default-book page count ──────────
//
// Inline TWO script tags in <head>:
//   #books-data  — the (single-book) books.json registry (label + totalPages)
//   #pages-data  — { totalPages } for the standard book, the bare minimum to paint
//                  first frame. The song/search indexes are fetched lazily from
//                  /books/standard/{pages,search-index}.json.
// #pages-data also carries bookVersion — the identity of the book this shell was built around.
// It is written AFTER emitBundleManifest below, because bookVersion is a content hash over the
// finished dist and cannot be known before every file exists.
const writeIndexHtml = (bookVersion) => {
  const inlineScripts =
    `  <script id="books-data" type="application/json">${JSON.stringify(booksManifest)}</script>\n` +
    `  <script id="pages-data" type="application/json">${JSON.stringify({ totalPages: defaultTotalPages, bookVersion })}</script>\n`;
  const htmlSrc = fs.readFileSync(path.join(srcDir, "index.html"), "utf8");
  fs.writeFileSync(
    path.join(distDir, "index.html"),
    htmlSrc.replace("</head>", `${inlineScripts}</head>`),
  );
};
// Write app.js (now that the page count exists) and a placeholder index.html, so BOTH exist before
// the manifest walk — the manifest must cover every shipped file, and a manifest that can omit one
// is a completeness gate that proves nothing. index.html is rewritten with the real bookVersion
// immediately after, and its manifest entry is recomputed then (emitBundleManifest, pass 2).
emitAppJs(defaultTotalPages);
writeIndexHtml(null);

// INERT BY DESIGN — this rule does nothing in production, and that is currently the SAFER
// outcome. Cloudflare Pages rejects any `_headers` path containing more than one `*`, and
// `/books/*/pages/*` has two, so Pages drops the rule at parse time and serves its own default
// `public, max-age=0, must-revalidate` for these images instead. Verified 2026-07-31 against
// signovivo.com, the raw Pages origin, and a throwaway preview deploy; `wrangler pages dev` logs
// it outright as an invalid header rule. (Placeholders are unlimited — only splats are capped at
// one — so the valid rewrite is `/books/:book/pages/*`, confirmed working on that preview deploy.)
//
// DO NOT apply that rewrite yet. Page filenames are stable across PDF revisions, so making
// `immutable` real would pin a revised page in every follower's browser cache for a YEAR with no
// operator remedy short of a cache purge plus a per-device storage clear — and pages DO get
// revised in place: build 377 / PR #257 edited assets/alvernia_manual_2.pdf and re-rendered ~290
// pages under unchanged page-NNN.webp names. M6 in docs/major-update-2026-07.md gives pages
// hash-keyed URLs, which is what makes `immutable` safe; enable it then, not before.
// The rule is kept (rather than deleted) as the marker for that M6 follow-up. See web/src/sw.js,
// whose cross-version page-cache fallback is governed by the same staleness question.
fs.writeFileSync(
  path.join(distDir, "_headers"),
  "/books/*/pages/*\n  Cache-Control: public, max-age=31536000, immutable\n",
);

// ─── bundle-manifest.json — the identity of this bundle ──────────────────────
//
// WHY. Until this file existed there was no way to ask a device "which book are you actually
// rendering?" The build badge answers a different question (the native SHELL's version.json
// number), and it can read v383 on a device rendering a book from months earlier — that is defect
// D1 in docs/choir-pdf-distribution-plan.md §3.2, demonstrated live on a simulator. The manifest is
// what makes the question answerable at all: it ships inside the IPA (release.sh copies web/dist to
// ios/WebBundle) AND on signovivo.com, so the baked book and the served book cannot disagree by
// construction.
//
// It also carries the evidence that hashing alone cannot supply. Every integrity check in this
// design measures the artifact against itself, so a WRONG-but-well-formed PDF passes all of them
// (red team A6). `sourcePdfPages` comes from pdfinfo — the PDF, not the render — so a render that
// silently drops pages is a mismatch rather than a consistent lie, and `sourcePdfSha256` pins which
// PDF this actually came from.
const toolVersion = (bin, args) => {
  // pdftoppm and cwebp both report version on stderr; merge both streams and take the first line.
  const r = spawnSync(bin, args, { encoding: "utf8" });
  const text = `${r.stdout || ""}${r.stderr || ""}`.trim().split("\n")[0] || "";
  return text.trim().slice(0, 120) || "unknown";
};

const walkFiles = (dir, base = dir) => {
  // WALK THE TREE. Never build this list in memory as files are written: a manifest that CAN omit a
  // file is a completeness gate that proves nothing (red team A6/NI7). Anything on disk is in the
  // manifest, or the manifest is wrong in a way the device will detect.
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...walkFiles(full, base));
    else if (entry.isFile()) out.push(path.relative(base, full).split(path.sep).join("/"));
  }
  return out;
};

const hashFile = (abs) => {
  const bytes = fs.readFileSync(abs);
  return {
    n: bytes.length,
    // h (sha256) derives bookVersion and drives the CI additive gate.
    h: crypto.createHash("sha256").update(bytes).digest("hex"),
    // m (md5) is what the DEVICE verifies: expo-file-system exposes getInfoAsync({ md5: true }) and
    // not sha256. md5 here is a CORRUPTION check only — HTTPS to our own origin is the
    // authenticity boundary. No pod, no CryptoKit, no Swift.
    m: crypto.createHash("md5").update(bytes).digest("hex"),
  };
};

const MANIFEST_NAME = "bundle-manifest.json";
// PAGES CONFIG — ships in the deploy directory, NOT fetchable over HTTP.
//
// Cloudflare Pages treats these as CONFIGURATION, not assets: wrangler drops `_headers`,
// `_redirects`, `_routes.json` and `_worker.js` from the upload walk and attaches them to the
// deployment instead. A GET for one therefore returns the SPA fallback — `200 text/html`, ~21 KB
// — not the file. That is HANDOFF lesson 5 ("HTTP 200 is not evidence a file exists") in its most
// expensive form, because the OTA consumes this list: src/bookUpdate.js queues EVERY manifest
// entry and verifyStaged demands exact size AND md5 for each. Listing `_headers` (70 B) made every
// device download the whole ~27 MB book and then fail `size:_headers` — forever, on every retry —
// and sync-worker renders that failure identically to a device that was never armed, so it is
// silent as well as permanent. Measured against signovivo.com 2026-08-03: 200 text/html 21841 B.
//
// They stay ON DISK (Pages needs them; the copy baked into ios/WebBundle is inert) but are
// excluded from `files` and from bookVersion — a `_headers` edit is not a new book.
// scripts/smoke-boot.mjs pins this with its OWN list rather than reading one back from the
// manifest, per HANDOFF lesson 1.
const NOT_FETCHABLE = new Set(["_headers", "_redirects", "_routes.json", "_worker.js"]);
// The lowest shell build allowed to run this bundle. Bump it ONLY when a bundle starts requiring
// native capability an older shell lacks; the device refuses to download a book it cannot run
// rather than installing one that boots broken. 1 = any shell.
/**
 * The oldest shell allowed to DOWNLOAD this book. Both gates read it: `shouldStage` refuses with
 * `shell-too-old` before a byte moves, and `stageBook` refuses again after fetching the manifest.
 *
 * RAISED TO 400 on 2026-08-05 (was 1, then briefly 398), and this is a safety gate, not a nicety. TWO separate native
 * fixes are required before a shell can survive an OTA, and a device missing EITHER is harmed by
 * being offered one:
 *
 *   <= 394  no bundle-manifest.json written into the staged bundle, so an applied book is
 *           unidentifiable, decideBundle rule 3 evicts it, activeBookVersion stays on the baked
 *           book, and the next check-in re-stages the whole ~26 MB. A permanent loop.
 *   <= 397  no allowingReadAccessToURL on the WebView, so an applied bundle can read index.html
 *           and is DENIED styles.css/app.js/lib/*. It renders as raw HTML, never reaches
 *           bridge-ready, and the watchdog quarantines a byte-perfect bundle and reverts.
 *   <= 399  verifyStaged still refuses EVERY shrink, so the 371-page book this gate ships with
 *           fails `shrank:374->371` — and a verify failure re-stages on the next check-in, so it
 *           is a ~26 MB loop, not a clean refusal.
 *
 * It was briefly set to 395 here — correct for the first defect, WRONG for the second, which would
 * have admitted exactly the build (395) that was measured breaking on the owner's iPhone.
 *
 * With this set, older devices never start: they are simply told nothing they can act on. THIS IS
 * WHAT MAKES A FLEET-WIDE ARM SAFE while the fleet is still mixed.
 *
 * RAISE THIS whenever a book depends on native behaviour an older shell lacks. Lowering it is what
 * needs justification, not raising it.
 */
const MIN_SHELL_BUILD = 400;

const emitBundleManifest = () => {
  // Pass 1 — hash everything on disk except the manifest itself (it does not exist yet) and
  // index.html. index.html is excluded from bookVersion ONLY, and the exclusion is load-bearing
  // rather than cosmetic: index.html carries bookVersion inline, so including it would make the
  // hash an input to itself. Excluding it is safe because index.html's book-dependent content is
  // exactly totalPages + bookVersion, and any book change that moves those also moves a page file.
  // It is still hashed into `files` below with its FINAL bytes, so the completeness/integrity gates
  // and the device's own verification cover it in full.
  const relPaths = walkFiles(distDir).filter((p) => p !== MANIFEST_NAME && !NOT_FETCHABLE.has(p));
  const hashes = new Map(relPaths.map((p) => [p, hashFile(path.join(distDir, p))]));

  const bookVersion = "bv_" + crypto
    .createHash("sha256")
    .update(
      relPaths
        .filter((p) => p !== "index.html")
        .sort()
        .map((p) => `${p}:${hashes.get(p).h}`)
        .join("\n"),
    )
    .digest("hex")
    .slice(0, 16);

  // Pass 2 — now index.html can be written with the real identity, and re-hashed.
  writeIndexHtml(bookVersion);
  hashes.set("index.html", hashFile(path.join(distDir, "index.html")));

  // Book facts read from the SOURCE PDF, independent of the render — this is the cross-check that
  // catches a render which silently lost pages, which no self-referential hash can.
  const info = spawnSync("pdfinfo", [BOOK_PDF_PATH], { encoding: "utf8" });
  const sourcePdfPages = Number((info.stdout?.match(/^Pages:\s+(\d+)/m) || [])[1]);
  if (!Number.isFinite(sourcePdfPages)) {
    throw new Error("bundle-manifest: pdfinfo could not read the source PDF page count.");
  }
  if (sourcePdfPages !== defaultTotalPages) {
    throw new Error(
      `bundle-manifest: the PDF has ${sourcePdfPages} pages but ${defaultTotalPages} were rendered. ` +
      "Refusing to publish a bundle whose book is not the book.",
    );
  }

  const pagesJson = JSON.parse(
    fs.readFileSync(path.join(booksDir, DEFAULT_BOOK, "pages.json"), "utf8"),
  );
  const songs = pagesJson.songIndex || [];

  const manifest = {
    schema: 1,
    bookVersion,
    totalPages: defaultTotalPages,
    builtFromShellBuild: Number(buildNumber) || 0,
    minShellBuild: MIN_SHELL_BUILD,
    generatedAt: new Date().toISOString(),
    sourcePdfSha256: crypto.createHash("sha256").update(fs.readFileSync(BOOK_PDF_PATH)).digest("hex"),
    sourcePdfPages,
    // Lets the additive gate prove that song N still opens the SAME page — a wrong-but-longer PDF
    // would otherwise read as "372 modified, 40 new" and look like an ordinary revision.
    songIndexDigest: crypto.createHash("sha256").update(JSON.stringify(songs)).digest("hex"),
    firstSong: songs.length ? songs[0].song : null,
    lastSong: songs.length ? songs[songs.length - 1].song : null,
    // Compact [song, page] pairs (~3 KB against a 27 MB bundle). The DIGEST alone cannot power the
    // additive gate's most important assertion — that song N still opens the SAME page — because it
    // changes whenever a song is legitimately appended. The pairs let the gate say exactly which
    // song moved, which is the difference between "a new PDF was added to the end" and "a different
    // edition was substituted and every song shifted by two pages."
    songPages: songs.map((s) => [s.song, s.page]),
    pagePadWidth: PAGE_PAD_WIDTH,
    // Recorded so a renderer change is a LOUD explicit override instead of 372 silent hash
    // differences. CI installs poppler/webp unpinned, so the runner and the build Mac can disagree.
    renderer: {
      dpi: PDF_RENDER_DPI,
      webpQuality: WEBP_QUALITY,
      pdftoppm: toolVersion("pdftoppm", ["-v"]),
      cwebp: toolVersion("cwebp", ["-version"]),
    },
    files: [...hashes.keys()].sort().map((p) => ({ p, ...hashes.get(p) })),
  };

  fs.writeFileSync(path.join(distDir, MANIFEST_NAME), JSON.stringify(manifest, null, 2));
  return manifest;
};

const bundleManifest = emitBundleManifest();

console.log(`\nBuild complete. Default book: ${DEFAULT_BOOK} (${defaultTotalPages} pages).`);
console.log(
  `  bundle-manifest.json: ${bundleManifest.bookVersion} · ${bundleManifest.files.length} files · ` +
  `shell ${bundleManifest.builtFromShellBuild} (min ${bundleManifest.minShellBuild})`,
);
console.log(`  renderer: ${bundleManifest.renderer.pdftoppm} | ${bundleManifest.renderer.cwebp}`);

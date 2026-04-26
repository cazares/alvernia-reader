import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { Scale, Note } from "tonal";

const rootDir = path.resolve(new URL("..", import.meta.url).pathname);
const srcDir = path.join(rootDir, "web", "src");
const distDir = path.join(rootDir, "web", "dist");
const pagesDir = path.join(distDir, "pages");
const thumbsDir = path.join(distDir, "thumbs");
const cacheVersion = (() => {
  const gitSha = spawnSync("git", ["rev-parse", "--short", "HEAD"], {
    cwd: rootDir,
    encoding: "utf8",
  });
  if (gitSha.status === 0 && gitSha.stdout.trim()) {
    return gitSha.stdout.trim();
  }
  return `${Date.now()}`;
})();

fs.rmSync(distDir, { recursive: true, force: true });
fs.mkdirSync(distDir, { recursive: true });
fs.mkdirSync(pagesDir, { recursive: true });
fs.mkdirSync(thumbsDir, { recursive: true });

fs.copyFileSync(path.join(srcDir, "styles.css"), path.join(distDir, "styles.css"));
const appSource = fs
  .readFileSync(path.join(srcDir, "app.js"), "utf8")
  .replaceAll("__CACHE_VERSION__", cacheVersion);
fs.writeFileSync(path.join(distDir, "app.js"), appSource);
fs.copyFileSync(path.join(srcDir, "manifest.webmanifest"), path.join(distDir, "manifest.webmanifest"));
// index.html is written later with inlined JSON data

fs.copyFileSync(path.join(rootDir, "assets", "icon.png"), path.join(distDir, "icon.png"));
const serviceWorkerSource = fs
  .readFileSync(path.join(srcDir, "sw.js"), "utf8")
  .replaceAll("__CACHE_VERSION__", cacheVersion);
fs.writeFileSync(path.join(distDir, "sw.js"), serviceWorkerSource);

const generateIcon = (size, outputName) => {
  const result = spawnSync(
    "sips",
    ["-z", String(size), String(size), path.join(rootDir, "assets", "icon.png"), "--out", path.join(distDir, outputName)],
    { stdio: "inherit" },
  );

  if (result.status !== 0) {
    throw new Error(`sips failed while generating ${outputName}`);
  }
};

generateIcon(192, "icon-192.png");
generateIcon(512, "icon-512.png");

const parsePositiveInt = (value, fallback) => {
  const n = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(n) && n > 0 ? n : fallback;
};

const parseJpegQuality = (value, fallback) => {
  const n = Number.parseInt(String(value ?? "").trim(), 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.min(100, n));
};

// Rendering pages from the PDF is the single biggest lever for sharpness.
// Defaults are intentionally "high res" to match the original native-PDF clarity.
const PDF_RENDER_DPI = parsePositiveInt(process.env.ALVERNIA_PDF_RENDER_DPI, 240);
const PDF_RENDER_JPEG_QUALITY = parseJpegQuality(process.env.ALVERNIA_PDF_RENDER_JPEG_QUALITY, 92);

const pdfPath = path.join(rootDir, "assets", "alvernia_manual_2.pdf");
const outputPrefix = path.join(pagesDir, "page");
const convert = spawnSync(
  "pdftoppm",
  ["-jpeg", "-jpegopt", `quality=${PDF_RENDER_JPEG_QUALITY}`, "-r", String(PDF_RENDER_DPI), pdfPath, outputPrefix],
  { stdio: "inherit" },
);

if (convert.status !== 0) {
  throw new Error(`pdftoppm failed with exit code ${convert.status ?? 1}`);
}

const pageFiles = fs
  .readdirSync(pagesDir)
  .filter((file) => /^page-\d+\.jpg$/.test(file))
  .sort((left, right) => left.localeCompare(right));

const parseNonNegativeInt = (value, fallback) => {
  const n = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isFinite(n) && n >= 0 ? n : fallback;
};

const THUMB_WIDTH = parsePositiveInt(process.env.ALVERNIA_THUMB_WIDTH, 320);
const THUMB_JPEG_QUALITY = parseJpegQuality(process.env.ALVERNIA_THUMB_JPEG_QUALITY, 70);
const THUMB_CONCURRENCY = Math.max(1, parseNonNegativeInt(process.env.ALVERNIA_THUMB_CONCURRENCY, 1) || 1);

// Generate thumbnail strip used by the native grid/browse UI.
// (The app depends on these existing in the offline-web bundle.)
console.log(`Generating ${pageFiles.length} thumbs at width=${THUMB_WIDTH}, quality=${THUMB_JPEG_QUALITY}...`);
const makeThumb = (file) => {
  const match = file.match(/page-(\d+)\.jpg$/);
  const num = match ? match[1] : null;
  if (!num) return;
  const src = path.join(pagesDir, file);
  const out = path.join(thumbsDir, `thumb-${num}.jpg`);
  const result = spawnSync(
    "sips",
    ["-Z", String(THUMB_WIDTH), "-s", "format", "jpeg", "-s", "formatOptions", String(THUMB_JPEG_QUALITY), src, "--out", out],
    { stdio: "pipe" },
  );
  if (result.status !== 0 || !fs.existsSync(out)) {
    throw new Error(`Failed generating thumb for ${file}`);
  }
};

if (THUMB_CONCURRENCY <= 1) {
  for (let i = 0; i < pageFiles.length; i++) {
    makeThumb(pageFiles[i]);
    if ((i + 1) % 50 === 0) process.stdout.write(`  ${i + 1}/${pageFiles.length}\n`);
  }
} else {
  // Simple chunked parallelism (still safe on laptops; avoid overwhelming).
  let i = 0;
  while (i < pageFiles.length) {
    const chunk = pageFiles.slice(i, i + THUMB_CONCURRENCY);
    for (const f of chunk) makeThumb(f);
    i += chunk.length;
    if (i % 50 === 0) process.stdout.write(`  ${i}/${pageFiles.length}\n`);
  }
}
console.log("Thumb generation done.");

const songIndexSource = fs.readFileSync(path.join(rootDir, "src", "alverniaManual2SongIndex.js"), "utf8");
const songIndex = [];
for (const match of songIndexSource.matchAll(/\[(\d+),\s*(\d+)\]/g)) {
  songIndex.push({ song: Number(match[1]), page: Number(match[2]) });
}

// ─── PDF Text Extraction ────────────────────────────────────────────────────

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

// ─── Song Title Overrides ────────────────────────────────────────────────────
// Manual corrections for songs where OCR title extraction is inaccurate

const TITLE_OVERRIDES = {
  24:  "Santo Juvenil",
  198: "Señor quien puede entrar",
  249: "Solamente de la Santa Trinidad",
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
  const majorNotes = new Set(Scale.get(`${tonic} major`).notes.map((n) => Note.simplify(n) || n));
  let score = 0;
  for (const r of roots) {
    const simplified = Note.simplify(r) || r;
    if (majorNotes.has(simplified)) score += 1;
  }
  // Small bonus when the first chord's root matches the tonic (home-key feel)
  const firstSimplified = Note.simplify(roots[0]) || roots[0];
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
  {
    id: "adviento",
    label: "Adviento",
    emoji: "🕯️",
    phrases: ["ven señor no tardes", "preparad el camino", "maranatha", "viene el señor", "tiempo de adviento", "luz en la oscuridad", "el señor viene", "ven señor jesucristo", "preparad los caminos"],
    words: ["adviento", "maranatha", "venida", "preparad", "vigilad", "anunciad", "espera", "llegará"],
  },
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
  {
    id: "funerales",
    label: "Funerales / Difuntos",
    emoji: "🌹",
    phrases: ["descansa en paz", "en la casa del padre", "vida eterna para", "aunque pase por el valle", "el señor es mi pastor", "no temas la muerte", "resurrección de los muertos"],
    words: ["difuntos", "funeral", "entierro", "eterno descanso", "fallecido", "cementerio", "velorio", "luto", "sepultura", "duelo"],
  },
  {
    id: "confirmacion",
    label: "Confirmación",
    emoji: "🔥",
    phrases: ["sellados por el espíritu", "confirmados en la fe", "ungidos por el espíritu", "vengan al altar de dios"],
    words: ["confirmación", "confirmados", "crismación", "unción", "ungido", "sello"],
  },
  {
    id: "primera_comunion",
    label: "Primera Comunión",
    emoji: "🍞",
    phrases: ["hoy recibo a jesús", "por primera vez", "primera comunión", "me acerco a ti señor", "vengo a ti por primera"],
    words: ["primera comunión", "primicias", "primera vez"],
  },
  // ── Functional / directorial themes ────────────────────────────────────────
  {
    id: "entrada",
    label: "Entrada / Inicio de Misa",
    emoji: "🚪",
    phrases: ["entremos en la casa del señor", "venid al templo", "venimos a adorarte", "hoy venimos ante ti", "entramos en tu presencia", "aquí estamos reunidos"],
    words: ["entramos", "venimos", "congregamos", "reunidos", "asamblea", "gathering"],
  },
  {
    id: "envio",
    label: "Envío / Final de Misa",
    emoji: "🙌",
    phrases: ["id a anunciar", "vayan en paz", "salid al mundo", "id por todo el mundo", "misión cumplida", "la misa ha terminado"],
    words: ["envío", "misión", "anunciad", "proclamad", "id", "marchad", "salid"],
  },
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
  {
    id: "santos",
    label: "Santos / Fiestas Patronales",
    emoji: "👑",
    phrases: ["fiesta de todos los santos", "comunión de los santos", "fiesta patronal", "intercede por nosotros"],
    words: ["santos", "mártires", "patrono", "patrona", "beatificación", "santoral", "fiesta", "intercesor", "interces"],
  },
];

// Normalize: NFD + strip accents + lowercase
const normalize = (text) =>
  text.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();

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

// ─── Enrich songIndex with titles + themes + keys + intro ────────────────────

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

// ─── Search Index ────────────────────────────────────────────────────────────

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
  path.join(distDir, "search-index.json"),
  JSON.stringify({ pages: searchIndexPages }),
);

// ─── Write manifests ─────────────────────────────────────────────────────────

const themeIndex = THEMES.map(({ id, label, emoji }) => ({ id, label, emoji }));

fs.writeFileSync(
  path.join(distDir, "pages.json"),
  JSON.stringify({ totalPages: pageFiles.length, songIndex, themeIndex }),
);

// Used by the native reader for fast title lookups without pulling the full songIndex.
const songTitles = {};
for (const entry of songIndex) {
  if (entry?.song && entry?.title) songTitles[String(entry.song)] = entry.title;
}
fs.writeFileSync(path.join(distDir, "song-titles.json"), JSON.stringify(songTitles));
fs.writeFileSync(path.join(distDir, "song-search-index.json"), JSON.stringify(songSearchIndex));

// Inject page manifest and search index into HTML for .webarchive compatibility
const pagesJson = JSON.stringify({ totalPages: pageFiles.length, songIndex, themeIndex });
const searchJson = JSON.stringify({ pages: searchIndexPages });
const inlineScripts =
  `  <script id="pages-data" type="application/json">${pagesJson}</script>\n` +
  `  <script id="search-data" type="application/json">${searchJson}</script>\n`;
const htmlSrc = fs.readFileSync(path.join(srcDir, "index.html"), "utf8");
fs.writeFileSync(
  path.join(distDir, "index.html"),
  htmlSrc.replace("</head>", `${inlineScripts}</head>`),
);

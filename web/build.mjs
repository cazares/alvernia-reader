import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const rootDir = path.resolve(new URL("..", import.meta.url).pathname);
const srcDir = path.join(rootDir, "web", "src");
const distDir = path.join(rootDir, "web", "dist");
const pagesDir = path.join(distDir, "pages");

fs.rmSync(distDir, { recursive: true, force: true });
fs.mkdirSync(distDir, { recursive: true });
fs.mkdirSync(pagesDir, { recursive: true });

for (const file of ["styles.css", "app.js", "manifest.webmanifest"]) {
  fs.copyFileSync(path.join(srcDir, file), path.join(distDir, file));
}
// index.html is written later with inlined JSON data

fs.copyFileSync(path.join(rootDir, "assets", "icon.png"), path.join(distDir, "icon.png"));
fs.copyFileSync(path.join(srcDir, "sw.js"), path.join(distDir, "sw.js"));

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

const pdfPath = path.join(rootDir, "assets", "alvernia_manual_2.pdf");
const outputPrefix = path.join(pagesDir, "page");
const convert = spawnSync(
  "pdftoppm",
  ["-jpeg", "-jpegopt", "quality=80", "-r", "144", pdfPath, outputPrefix],
  { stdio: "inherit" },
);

if (convert.status !== 0) {
  throw new Error(`pdftoppm failed with exit code ${convert.status ?? 1}`);
}

const pageFiles = fs
  .readdirSync(pagesDir)
  .filter((file) => /^page-\d+\.jpg$/.test(file))
  .sort((left, right) => left.localeCompare(right));

const songIndexSource = fs.readFileSync(path.join(rootDir, "src", "alverniaManual2SongIndex.js"), "utf8");
const songIndex = [];
for (const match of songIndexSource.matchAll(/song:\s*(\d+),\s*page:\s*(\d+)/g)) {
  songIndex.push({ song: Number(match[1]), page: Number(match[2]) });
}

fs.writeFileSync(
  path.join(distDir, "pages.json"),
  JSON.stringify({ totalPages: pageFiles.length, songIndex }),
);

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
const searchIndexPages = pageFiles.map((file, idx) => {
  const pageNum = idx + 1;
  const rawText = pageTextsRaw[idx] || "";
  const text = rawText.replace(/\s+/g, " ").trim().slice(0, 600);
  return { page: pageNum, text };
}).filter((entry) => entry.text.length > 5);

fs.writeFileSync(
  path.join(distDir, "search-index.json"),
  JSON.stringify({ pages: searchIndexPages }),
);

// Inject page manifest and search index into HTML for .webarchive compatibility
const pagesJson = JSON.stringify({ totalPages: pageFiles.length, songIndex });
const searchJson = JSON.stringify({ pages: searchIndexPages });
const inlineScripts =
  `  <script id="pages-data" type="application/json">${pagesJson}</script>\n` +
  `  <script id="search-data" type="application/json">${searchJson}</script>\n`;
const htmlSrc = fs.readFileSync(path.join(srcDir, "index.html"), "utf8");
fs.writeFileSync(
  path.join(distDir, "index.html"),
  htmlSrc.replace("</head>", `${inlineScripts}</head>`),
);

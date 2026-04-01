/**
 * Generates:
 * 1. A fully self-contained signo-vino-offline.html for AirDrop/Safari use.
 * 2. A small native HTML shell plus bundled page-image assets for the iOS app.
 *
 * Run AFTER the main build:
 *   node web/build.mjs && node web/build-offline.mjs
 */

import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const rootDir = path.resolve(new URL("..", import.meta.url).pathname);
const distDir = path.join(rootDir, "web", "dist");
const pagesDir = path.join(distDir, "pages");
const tmpDir = path.join(rootDir, "web", ".offline-tmp");
const nativePagesDir = path.join(rootDir, "assets", "offline-pages");
const nativeHtmlPath = path.join(rootDir, "assets", "signo-vino-native.html");
const nativeAssetsModulePath = path.join(rootDir, "src", "offlineWebAssets.js");
const nativeAssetsTypesPath = path.join(rootDir, "src", "offlineWebAssets.d.ts");

fs.mkdirSync(tmpDir, { recursive: true });

const pageFiles = fs
  .readdirSync(pagesDir)
  .filter((f) => /^page-\d+\.jpg$/.test(f))
  .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

console.log(`Re-compressing ${pageFiles.length} pages at quality=55...`);

const pagesData = {};
for (let i = 0; i < pageFiles.length; i++) {
  const file = pageFiles[i];
  const num = parseInt(file.match(/(\d+)/)[1], 10);
  const src = path.join(pagesDir, file);
  const tmp = path.join(tmpDir, file);

  const result = spawnSync(
    "sips",
    ["-s", "format", "jpeg", "-s", "formatOptions", "55", src, "--out", tmp],
    { stdio: "pipe" },
  );
  const recompressedExists = fs.existsSync(tmp);
  if (result.status !== 0 || !recompressedExists) {
    const fallback = fs.readFileSync(src);
    pagesData[num] = `data:image/jpeg;base64,${fallback.toString("base64")}`;
    continue;
  }

  const data = fs.readFileSync(tmp);
  pagesData[num] = `data:image/jpeg;base64,${data.toString("base64")}`;

  if ((i + 1) % 50 === 0) process.stdout.write(`  ${i + 1}/${pageFiles.length}\n`);
}

fs.rmSync(tmpDir, { recursive: true, force: true });
console.log("Re-compression done.");

// Read built dist files (index.html already has inlined pages.json + search-index.json)
let html = fs.readFileSync(path.join(distDir, "index.html"), "utf8");
const css = fs.readFileSync(path.join(distDir, "styles.css"), "utf8");
const js = fs.readFileSync(path.join(distDir, "app.js"), "utf8");

// Inline CSS
html = html.replace(
  '<link rel="stylesheet" href="/styles.css" />',
  `<style>\n${css}\n</style>`,
);

// Inject OFFLINE_PAGES and inline JS (before </body>)
const offlineScript = `<script>window.OFFLINE_PAGES=${JSON.stringify(pagesData)};</script>`;
const appScript = `<script>\n${js}\n</script>`;
html = html.replace(
  '<script defer src="/app.js"></script>',
  `${offlineScript}\n    ${appScript}`,
);

// Point initial <img src> at page 2 data URI so it's immediately visible
html = html.replace(
  'src="/pages/page-002.jpg"',
  `src="${pagesData[2]}"`,
);

// Strip external resource references that don't work offline
html = html
  .replace(/\s*<link rel="manifest"[^>]*>\n?/, "\n")
  .replace(/\s*<link rel="icon"[^>]*>\n?/, "\n")
  .replace(/\s*<link rel="apple-touch-icon"[^>]*>\n?/, "\n");

const outPath = path.join(distDir, "signo-vino-offline.html");
fs.writeFileSync(outPath, html);

const sizeMB = (fs.statSync(outPath).size / 1024 / 1024).toFixed(1);
console.log(`\nWrote ${outPath}`);
console.log(`File size: ${sizeMB} MB`);
console.log("\nAirDrop signo-vino-offline.html to iPad, tap it in Files → opens in Safari.");

let nativeHtml = fs.readFileSync(path.join(distDir, "index.html"), "utf8");
nativeHtml = nativeHtml.replace(
  '<link rel="stylesheet" href="/styles.css" />',
  `<style>\n${css}\n</style>`,
);
nativeHtml = nativeHtml.replace(
  '<script defer src="/app.js"></script>',
  `<script>\n${js}\n</script>`,
);
nativeHtml = nativeHtml
  .replace(/\s*<link rel="manifest"[^>]*>\n?/, "\n")
  .replace(/\s*<link rel="icon"[^>]*>\n?/, "\n")
  .replace(/\s*<link rel="apple-touch-icon"[^>]*>\n?/, "\n")
  .replace('src="/pages/page-002.jpg"', 'src=""');

fs.writeFileSync(nativeHtmlPath, nativeHtml);
console.log(`Wrote ${nativeHtmlPath}`);

fs.rmSync(nativePagesDir, { recursive: true, force: true });
fs.mkdirSync(nativePagesDir, { recursive: true });
for (const file of pageFiles) {
  fs.copyFileSync(path.join(pagesDir, file), path.join(nativePagesDir, file));
}
console.log(`Copied ${pageFiles.length} page images to ${nativePagesDir}`);

const moduleLines = [
  'const OFFLINE_READER_HTML_MODULE = require("../assets/signo-vino-native.html");',
  "",
  "const OFFLINE_PAGE_MODULES = {",
  ...pageFiles.map((file) => {
    const match = file.match(/(\d+)/);
    const pageNumber = match ? Number.parseInt(match[1], 10) : 0;
    return `  ${pageNumber}: require("../assets/offline-pages/${file}"),`;
  }),
  "};",
  "",
  "module.exports = {",
  "  OFFLINE_READER_HTML_MODULE,",
  "  OFFLINE_PAGE_MODULES,",
  "};",
  "",
];
fs.writeFileSync(nativeAssetsModulePath, moduleLines.join("\n"));

const typeLines = [
  "export const OFFLINE_READER_HTML_MODULE: number;",
  "export const OFFLINE_PAGE_MODULES: Record<number, number>;",
  "",
];
fs.writeFileSync(nativeAssetsTypesPath, typeLines.join("\n"));
console.log(`Wrote ${nativeAssetsModulePath}`);

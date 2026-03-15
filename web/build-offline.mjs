/**
 * Generates a fully self-contained alvernia-offline.html for AirDrop to iPad.
 *
 * Run AFTER the main build:
 *   node web/build.mjs && node web/build-offline.mjs
 *
 * The output file has everything embedded — CSS, JS, and all 368 page images
 * as base64 data URIs (re-compressed to ~q55 to reduce size). Open it on iPad
 * by tapping it in Files after AirDrop; Safari opens it with full offline support.
 */

import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const rootDir = path.resolve(new URL("..", import.meta.url).pathname);
const distDir = path.join(rootDir, "web", "dist");
const pagesDir = path.join(distDir, "pages");
const tmpDir = path.join(rootDir, "web", ".offline-tmp");

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
  if (result.status !== 0) throw new Error(`sips failed on ${file}`);

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

// Point initial <img src> at page 1 data URI so it's immediately visible
html = html.replace(
  'src="/pages/page-001.jpg"',
  `src="${pagesData[1]}"`,
);

// Strip external resource references that don't work offline
html = html
  .replace(/\s*<link rel="manifest"[^>]*>\n?/, "\n")
  .replace(/\s*<link rel="icon"[^>]*>\n?/, "\n")
  .replace(/\s*<link rel="apple-touch-icon"[^>]*>\n?/, "\n");

const outPath = path.join(distDir, "alvernia-offline.html");
fs.writeFileSync(outPath, html);

const sizeMB = (fs.statSync(outPath).size / 1024 / 1024).toFixed(1);
console.log(`\nWrote ${outPath}`);
console.log(`File size: ${sizeMB} MB`);
console.log("\nAirDrop alvernia-offline.html to iPad, tap it in Files → opens in Safari.");

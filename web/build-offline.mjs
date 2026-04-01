/**
 * Generates:
 * 1. A fully self-contained signo-vino-offline.html for AirDrop/Safari use.
 * 2. A native offline-web bundle for the iOS app.
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
const nativeBundleDir = path.join(rootDir, "assets", "offline-web");
const nativeAssetsModulePath = path.join(rootDir, "src", "offlineWebBundle.js");
const nativeAssetsTypesPath = path.join(rootDir, "src", "offlineWebBundle.d.ts");
const gitSha = spawnSync("git", ["rev-parse", "--short", "HEAD"], {
  cwd: rootDir,
  encoding: "utf8",
});
const bundleVersion = gitSha.status === 0 && gitSha.stdout.trim()
  ? gitSha.stdout.trim()
  : `${Date.now()}`;

const copyDir = (sourceDir, targetDir) => {
  fs.mkdirSync(targetDir, { recursive: true });
  for (const entry of fs.readdirSync(sourceDir, { withFileTypes: true })) {
    const sourcePath = path.join(sourceDir, entry.name);
    const targetPath = path.join(targetDir, entry.name);
    if (entry.isDirectory()) {
      copyDir(sourcePath, targetPath);
    } else {
      fs.copyFileSync(sourcePath, targetPath);
    }
  }
};

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

fs.rmSync(nativeBundleDir, { recursive: true, force: true });
copyDir(distDir, nativeBundleDir);

const nativeIndexPath = path.join(nativeBundleDir, "index.html");
let nativeHtml = fs.readFileSync(nativeIndexPath, "utf8");
nativeHtml = nativeHtml
  .replaceAll('href="/', 'href="')
  .replaceAll('src="/', 'src="');
fs.writeFileSync(nativeIndexPath, nativeHtml);
console.log(`Wrote ${nativeIndexPath}`);

const bundleFiles = [];
const walkBundle = (dir, relativeDir = "") => {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const relativePath = path.posix.join(relativeDir, entry.name);
    const absolutePath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walkBundle(absolutePath, relativePath);
    } else {
      bundleFiles.push(relativePath);
    }
  }
};
walkBundle(nativeBundleDir);

const moduleLines = [
  `const OFFLINE_WEB_BUNDLE_VERSION = ${JSON.stringify(bundleVersion)};`,
  "",
  "const OFFLINE_WEB_BUNDLE_ASSETS = {",
  ...bundleFiles.map((relativePath) => `  ${JSON.stringify(relativePath)}: require(${JSON.stringify(`../assets/offline-web/${relativePath}`)}),`),
  "};",
  "",
  "module.exports = {",
  "  OFFLINE_WEB_BUNDLE_VERSION,",
  "  OFFLINE_WEB_BUNDLE_ASSETS,",
  "};",
  "",
];
fs.writeFileSync(nativeAssetsModulePath, moduleLines.join("\n"));

const typeLines = [
  "export const OFFLINE_WEB_BUNDLE_VERSION: string;",
  "export const OFFLINE_WEB_BUNDLE_ASSETS: Record<string, number>;",
  "",
];
fs.writeFileSync(nativeAssetsTypesPath, typeLines.join("\n"));
console.log(`Wrote ${nativeAssetsModulePath}`);

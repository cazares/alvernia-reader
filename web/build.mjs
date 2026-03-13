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

for (const file of ["index.html", "styles.css", "app.js", "manifest.webmanifest"]) {
  fs.copyFileSync(path.join(srcDir, file), path.join(distDir, file));
}

fs.copyFileSync(path.join(rootDir, "assets", "icon.png"), path.join(distDir, "icon.png"));
fs.copyFileSync(path.join(srcDir, "sw.js"), path.join(distDir, "sw.js"));

const generateIcon = (size, outputName) => {
  const iconPath = path.join(rootDir, "assets", "icon.png");
  const outPath = path.join(distDir, outputName);

  // Try sips (macOS), fall back to ImageMagick convert (Linux)
  let result = spawnSync("sips", ["-z", String(size), String(size), iconPath, "--out", outPath], { stdio: "inherit" });
  if (result.status !== 0) {
    result = spawnSync("convert", [iconPath, "-resize", `${size}x${size}`, outPath], { stdio: "inherit" });
  }

  if (result.status !== 0) {
    throw new Error(`Icon generation failed for ${outputName}`);
  }
};

generateIcon(192, "icon-192.png");
generateIcon(512, "icon-512.png");

const pdfPath = path.join(rootDir, "assets", "alvernia_manual_2.pdf");
const outputPrefix = path.join(pagesDir, "page");
const convert = spawnSync(
  "pdftoppm",
  ["-jpeg", "-jpegopt", "quality=82", "-r", "144", pdfPath, outputPrefix],
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

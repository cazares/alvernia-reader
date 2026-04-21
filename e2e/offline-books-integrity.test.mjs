import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..");
const require = createRequire(import.meta.url);

const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8"));

test("offline standard book bundle has sentinel page assets", () => {
  const pagesJson = readJson(path.join(ROOT, "assets", "offline-web", "pages.json"));
  const total = Number(pagesJson.totalPages || 0);
  assert.ok(total > 0, `standard totalPages must be > 0`);

  const pagesDir = path.join(ROOT, "assets", "offline-web", "pages");
  const file1 = path.join(pagesDir, "page-001.jpg");
  const file2 = path.join(pagesDir, `page-${String(Math.min(total, 2)).padStart(3, "0")}.jpg`);
  const fileLast = path.join(pagesDir, `page-${String(total).padStart(3, "0")}.jpg`);
  assert.ok(fs.existsSync(file1), `Missing standard page asset: ${file1}`);
  assert.ok(fs.existsSync(file2), `Missing standard page asset: ${file2}`);
  assert.ok(fs.existsSync(fileLast), `Missing standard page asset: ${fileLast}`);
});

test("offline non-standard book bundles exist and page counts match", () => {
  const ids = ["hymns-1", "hymns-2", "hymns-4"];
  for (const id of ids) {
    const dir = path.join(ROOT, "assets", "offline-books", id);
    const pagesDir = path.join(dir, "pages");
    const pagesJsonPath = path.join(dir, "pages.json");
    assert.ok(fs.existsSync(dir), `missing bundle dir: ${dir}`);
    assert.ok(fs.existsSync(pagesDir), `missing pages dir: ${pagesDir}`);
    assert.ok(fs.existsSync(pagesJsonPath), `missing pages.json: ${pagesJsonPath}`);
    const pagesJson = readJson(pagesJsonPath);
    const total = Number(pagesJson.totalPages || 0);
    assert.ok(total > 0, `${id} totalPages must be > 0`);
    const jpgCount = fs.readdirSync(pagesDir).filter((f) => /^page-\d+\.jpg$/.test(f)).length;
    assert.equal(jpgCount, total, `${id} pages/*.jpg (${jpgCount}) must match pages.json.totalPages (${total})`);
  }
});

test("offline non-standard Metro asset maps include every page and thumbnail", () => {
  const ids = ["hymns-1", "hymns-2", "hymns-4"];
  for (const id of ids) {
    const pagesJson = readJson(path.join(ROOT, "assets", "offline-books", id, "pages.json"));
    const total = Number(pagesJson.totalPages || 0);
    const assetMap = fs.readFileSync(path.join(ROOT, "src", `offlineBookAssets.${id}.js`), "utf8");
    assert.match(assetMap, new RegExp(`pages/page-${String(total).padStart(3, "0")}\\.jpg`), `${id} asset map must include final page`);
    assert.match(assetMap, new RegExp(`thumbs/thumb-${String(total).padStart(3, "0")}\\.jpg`), `${id} asset map must include final thumb`);
  }
});

test("offline hymns-4 has OCR-derived song titles for Todas", () => {
  const titles = readJson(path.join(ROOT, "assets", "offline-books", "hymns-4", "song-titles.json"));
  const index = readJson(path.join(ROOT, "assets", "offline-books", "hymns-4", "song-search-index.json"));
  assert.ok(Object.keys(titles).length >= 80, "hymns-4 song-titles.json should have a meaningful number of entries");
  assert.ok(Array.isArray(index) && index.length >= 80, "hymns-4 song-search-index.json should have a meaningful number of entries");
  const anyTitle = Object.values(titles).find((v) => typeof v === "string" && v.trim().length >= 5);
  assert.ok(anyTitle, "hymns-4 should have at least one non-empty title");
});

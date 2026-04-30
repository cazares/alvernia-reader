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

test("offline standard book bundle has sentinel metadata", () => {
  const pagesJson = readJson(path.join(ROOT, "assets", "standard", "pages.json"));
  const total = Number(pagesJson.totalPages || 0);
  assert.ok(total > 0, `standard totalPages must be > 0`);

  const titlesPath = path.join(ROOT, "assets", "standard", "song-titles.json");
  const indexPath = path.join(ROOT, "assets", "standard", "song-search-index.json");
  assert.ok(fs.existsSync(titlesPath), `Missing standard song-titles.json: ${titlesPath}`);
  assert.ok(fs.existsSync(indexPath), `Missing standard song-search-index.json: ${indexPath}`);
});

test("offline non-standard book metadata exists (hymns-4)", () => {
  const pagesJsonPath = path.join(ROOT, "assets", "standard", "hymns-4-pages.json");
  assert.ok(fs.existsSync(pagesJsonPath), `missing hymns-4 pages json: ${pagesJsonPath}`);
});

test("offline hymns-4 has OCR-derived song titles and search index", () => {
  const titles = readJson(path.join(ROOT, "assets", "standard", "hymns-4-song-titles.json"));
  const index = readJson(path.join(ROOT, "assets", "standard", "hymns-4-song-search-index.json"));
  assert.ok(Object.keys(titles).length >= 20, "hymns-4 song-titles.json should have entries");
  assert.ok(Array.isArray(index) && index.length >= 20, "hymns-4 song-search-index.json should have entries");
});

test("standard book pages.json carries a valid totalPages integer", () => {
  const pagesJson = readJson(path.join(ROOT, "assets", "standard", "pages.json"));
  assert.ok(Number.isFinite(Number(pagesJson.totalPages)), "standard totalPages must be numeric");
});

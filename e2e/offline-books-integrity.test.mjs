import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..");

const readJson = (p) => JSON.parse(fs.readFileSync(p, "utf8"));

test("offline non-standard book bundles exist and page counts match", () => {
  const ids = ["hymns-1", "hymns-2", "hymns-3", "hymns-4"];
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

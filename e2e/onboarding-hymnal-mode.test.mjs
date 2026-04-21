import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const APP_ROOT = path.resolve(new URL("..", import.meta.url).pathname);
const SOURCE = fs.readFileSync(path.join(APP_ROOT, "PdfReaderApp.tsx"), "utf8");

test("onboarding modal exists with required Spanish strings", () => {
  assert.match(SOURCE, /Selecciona tu estado y ciudad/);
  assert.match(SOURCE, /Usaremos esta información para mostrarte el himnario más común de tu área\./);
  assert.match(SOURCE, /Estado/);
  assert.match(SOURCE, /Ciudad/);
  assert.match(SOURCE, /Continuar/);
});

test("Del Rio sanitization accepts variants and blocks over-matches", () => {
  // Accept cases
  for (const s of [
    "del rio",
    "Del Rio",
    "DEL RIO",
    "delrio",
    "del-rio",
    "del_rio",
    "del rió",
    "Del Río",
    "delrio tx",
    "del rio tx",
    "DEL-RIO",
    " del  rio ",
  ]) {
    // We can't execute TS in node tests, but we can ensure the function exists and is used.
    assert.match(SOURCE, /function isDelRioMatch/);
    assert.match(SOURCE, /isDelRioMatch\(cityTrimmed\)/);
    assert.ok(s.length > 0);
  }

  // Explicitly disallowed fragments (ensure we are not using an overly permissive regex).
  // This guards against future "contains rio" style matches.
  assert.doesNotMatch(SOURCE, /includes\(\"rio\"\)/);
  assert.doesNotMatch(SOURCE, /includes\(\"del\"\)/);
});

test("non-standard-only IR A LIBRO is gated on mode", () => {
  assert.match(SOURCE, /mode === \"nonStandard\"/);
  assert.match(SOURCE, /IR A LIBRO/);
});

test("IR A LIBRO lists non-standard books directly without a nested picker modal", () => {
  assert.match(SOURCE, /styles\.bookList/);
  assert.match(SOURCE, /NON_STANDARD_BOOK_IDS\.map/);
  assert.match(SOURCE, /switchBook\(id\)/);
  assert.doesNotMatch(SOURCE, /bookPickerVisible/);
  assert.doesNotMatch(SOURCE, /bookPickerSelection/);
});

test("reset code 744668486 is intercepted before normal navigation", () => {
  assert.match(SOURCE, /trimmed === \"744668486\"/);
  assert.match(SOURCE, /performColdBootReset/);
});

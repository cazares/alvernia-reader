import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const SRC = fs.readFileSync("web/build.mjs", "utf8");
const keyFn = SRC.slice(SRC.indexOf("const renderCacheKey"), SRC.indexOf("const pdfPageCount"));

// Rendering the book was ~87s of every publish, and a ROLLBACK paid all of it to reproduce pages it
// had already rendered byte-for-byte. The cache makes that 0.8s. The whole risk of the change is one
// thing: a cache that returns pixels which are not the pixels this build would have produced would
// ship the wrong music to the iPads and look perfectly healthy doing it. These tests guard the key.

test("the cache key covers every input that changes the output pixels", () => {
  assert.match(keyFn, /readFileSync\(pdfPath\)/, "the PDF's own bytes are not in the key");
  for (const [what, re] of [
    ["render DPI", /PDF_RENDER_DPI/],
    ["WebP quality", /WEBP_QUALITY/],
    ["filename pad width", /PAGE_PAD_WIDTH/],
    ["the pdftoppm version", /pdftoppm/],
    ["the cwebp version", /cwebp/],
  ]) {
    assert.match(keyFn, re, `${what} is missing from the cache key — two different books collide`);
  }
});

test("the renderer VERSIONS are in the key, not just the tool names", () => {
  // CI installs poppler and webp unpinned, so the runner and the build Mac legitimately disagree.
  // A key that named the tools without their versions would serve one machine's pixels as the other's.
  assert.match(keyFn, /toolVersion\("pdftoppm"/, "pdftoppm's VERSION is not consulted");
  assert.match(keyFn, /toolVersion\("cwebp"/, "cwebp's VERSION is not consulted");
});

test("toolVersion is defined BEFORE the cache key uses it", () => {
  // Regression: buildBook() runs at module top level, so a `const` helper declared further down the
  // file is still in its temporal dead zone when the key is built. This threw ReferenceError on the
  // very first cold run. `node --check` does NOT catch it — only executing the build does.
  const def = SRC.indexOf("const toolVersion =");
  const use = SRC.indexOf("const renderCacheKey");
  assert.ok(def > 0 && use > 0, "could not locate both toolVersion and renderCacheKey");
  assert.ok(def < use, "toolVersion is declared after the cache key — cold builds die in the TDZ");
});

test("an incomplete cache directory is re-rendered, never served", () => {
  // An interrupted build or a full disk leaves a partial directory. Serving it would publish a book
  // with holes in it, and every completeness check downstream runs on what the cache handed over.
  const fn = SRC.slice(SRC.indexOf("const renderPagesToWebp"), SRC.indexOf("// ─── Song Title"));
  assert.match(fn, /cached\.length === expected/, "a cache hit is not checked against the page count");
  assert.match(fn, /pdfPageCount/, "nothing establishes the expected page count independently");
});

test("the cache is populated only after a COMPLETE render", () => {
  const fn = SRC.slice(SRC.indexOf("const renderPagesToWebp"), SRC.indexOf("// ─── Song Title"));
  const at = fn.indexOf("mkdirSync(cacheDir");
  assert.ok(at > 0, "the cache is never populated");
  assert.match(fn.slice(0, at), /out\.length === expected/, "a partial render can poison the cache");
});

test("every page is verified to exist after the parallel encode", () => {
  // xargs runs cwebp across all cores and reports one aggregate status. A partial encode is the
  // failure that would ship a gap, so each output file is checked individually.
  const fn = SRC.slice(SRC.indexOf("const encodePngsToWebp"), SRC.indexOf("// Renders `pdfPath`"));
  assert.match(fn, /existsSync\(pairs\[i\]\)/, "nothing proves each page actually encoded");
  assert.match(fn, /throw new Error/, "a failed encode does not stop the build");
});

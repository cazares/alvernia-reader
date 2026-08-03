// Tests for scripts/additive-gate.mjs — the guard that build 377 / PR #257 needed.
//
// These assert the gate FIRES, not just that it passes on good input. A gate is only worth its
// runtime if its red path is exercised: the whole failure mode being defended against is a change
// that every other check in the pipeline reports as fine, so "it was green" proves nothing unless
// we also know it is capable of going red.
import test from "node:test";
import assert from "node:assert/strict";
import { compareManifests, OVERRIDE_PHRASE } from "../scripts/additive-gate.mjs";

const page = (n, h) => ({ p: `books/standard/pages/page-${String(n).padStart(3, "0")}.webp`, n: 100, h, m: "m" });

// A minimal well-formed manifest: 3 pages, 2 songs, a shell file.
const mk = (over = {}) => ({
  schema: 1,
  bookVersion: "bv_0000000000000000",
  totalPages: 3,
  pagePadWidth: 3,
  renderer: { dpi: 115, webpQuality: 60, pdftoppm: "pdftoppm version 26.04.0", cwebp: "1.6.0" },
  songPages: [[1, 1], [2, 2]],
  files: [page(1, "h1"), page(2, "h2"), page(3, "h3"), { p: "app.js", n: 10, h: "shell", m: "m" }],
  ...over,
});

test("identical manifests are additive", () => {
  const { violations } = compareManifests(mk(), mk());
  assert.deepEqual(violations, []);
});

test("appending a page is additive — the whole point", () => {
  const next = mk({
    totalPages: 4,
    bookVersion: "bv_1111111111111111",
    files: [...mk().files, page(4, "h4")],
  });
  const { violations, additions } = compareManifests(mk(), next);
  assert.deepEqual(violations, []);
  assert.equal(additions.pages, 1);
});

test("appending a page AND a song is additive", () => {
  const next = mk({
    totalPages: 4,
    songPages: [[1, 1], [2, 2], [3, 4]],
    files: [...mk().files, page(4, "h4")],
  });
  const { violations, additions } = compareManifests(mk(), next);
  assert.deepEqual(violations, []);
  assert.equal(additions.songs, 1);
});

test("shell files may change freely — they are not the immutable surface", () => {
  const next = mk({ files: mk().files.map((f) => (f.p === "app.js" ? { ...f, h: "different" } : f)) });
  assert.deepEqual(compareManifests(mk(), next).violations, []);
});

// ── The red paths. This is the PR #257 class. ────────────────────────────────

test("FIRES: a published page changed in place (the build-377 defect)", () => {
  const next = mk({ files: mk().files.map((f) => (f.p.endsWith("page-002.webp") ? { ...f, h: "REVISED" } : f)) });
  const { violations } = compareManifests(mk(), next);
  assert.equal(violations.length, 1);
  assert.match(violations[0], /CHANGED IN PLACE/);
  assert.match(violations[0], /page-002\.webp/);
});

test("FIRES: a page changed in place even though the page COUNT is identical", () => {
  // pdfinfo cannot see this — same page count, self-consistent book. That is exactly why #257 shipped.
  const next = mk({ files: mk().files.map((f) => (/page-00[12]/.test(f.p) ? { ...f, h: "x" } : f)) });
  const { violations } = compareManifests(mk(), next);
  assert.equal(next.totalPages, mk().totalPages);
  assert.match(violations.join(" "), /2 published page\(s\) CHANGED IN PLACE/);
});

test("FIRES: a published page disappeared", () => {
  const next = mk({ totalPages: 3, files: mk().files.filter((f) => !f.p.endsWith("page-002.webp")) });
  const v = compareManifests(mk(), next).violations.join(" | ");
  assert.match(v, /DISAPPEARED/);
  assert.match(v, /omits page/); // and the self-consistency check catches it independently
});

test("FIRES: the book shrank", () => {
  const next = mk({ totalPages: 2, files: mk().files.filter((f) => !f.p.endsWith("page-003.webp")) });
  assert.match(compareManifests(mk(), next).violations.join(" | "), /book SHRANK: 3 → 2/);
});

test("FIRES: a substituted edition where every song shifted (red team A6)", () => {
  // The dangerous case: a wrong-but-longer PDF. Page hashes alone read as an ordinary big revision;
  // the song→page mapping is what names it correctly.
  const next = mk({
    totalPages: 5,
    songPages: [[1, 3], [2, 4]],
    files: [page(1, "z1"), page(2, "z2"), page(3, "z3"), page(4, "z4"), page(5, "z5"), { p: "app.js", n: 10, h: "shell", m: "m" }],
  });
  const v = compareManifests(mk(), next).violations.join(" | ");
  assert.match(v, /no longer open the same page/);
  assert.match(v, /song 1: p1 → p3/);
});

test("FIRES: a song was removed", () => {
  const next = mk({ songPages: [[1, 1]] });
  assert.match(compareManifests(mk(), next).violations.join(" | "), /song 2 REMOVED/);
});

test("FIRES: renderer drift, so the hash comparison spans two encoders", () => {
  const next = mk({ renderer: { ...mk().renderer, cwebp: "1.7.0" } });
  assert.match(compareManifests(mk(), next).violations.join(" | "), /renderer\.cwebp changed/);
});

test("FIRES: pad width change — one integer that renames every page URL", () => {
  const next = mk({ pagePadWidth: 4 });
  assert.match(compareManifests(mk(), next).violations.join(" | "), /RENAMES every page URL/);
});

test("FIRES: manifest omits a page below totalPages", () => {
  const next = mk({ totalPages: 5, files: [...mk().files, page(5, "h5")] }); // 4 missing
  assert.match(compareManifests(mk(), next).violations.join(" | "), /omits page\(s\) 1\.\.totalPages: 4/);
});

test("a missing manifest is a violation, never a silent pass", () => {
  assert.equal(compareManifests(null, mk()).violations.length, 1);
  assert.equal(compareManifests(mk(), null).violations.length, 1);
});

test("the override phrase is a specific typed sentence, not a truthy flag", () => {
  // Guards against `ADDITIVE_OVERRIDE=1` becoming muscle memory.
  assert.equal(OVERRIDE_PHRASE, "yes I am changing published pages");
  assert.ok(OVERRIDE_PHRASE.includes(" "));
});

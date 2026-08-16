import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import crypto from "node:crypto";
import { spawnSync } from "node:child_process";

// scripts/splice-song-pages.py edits assets/songbook.pdf — the single highest-stakes artifact in
// the repo, since a bad page reaches ~8 iPads in a building with no internet and no remedy. These
// are BEHAVIOURAL tests against real PDFs, not source-text assertions: the failure that matters
// here ("the other 370 pages came out different") cannot be seen by reading the source.
//
// They are skipped, loudly, when pikepdf/Pillow are absent — CI installs poppler and webp but no
// Python PDF stack, and a red CI on a missing optional toolchain teaches everyone to ignore CI.
// Add `pip3 install pikepdf pillow` to .github/workflows/ci.yml to make them execute there.

const TOOL = "scripts/splice-song-pages.py";
const have = (mods) =>
  spawnSync("python3", ["-c", `import ${mods}`], { encoding: "utf8" }).status === 0;
const SKIP = !have("pikepdf, PIL")
  ? "pikepdf/Pillow not installed — `pip3 install pikepdf pillow` to run these"
  : spawnSync("which", ["pdftoppm"]).status !== 0
    ? "poppler (pdftoppm) not installed"
    : false;

const py = (code) => {
  const r = spawnSync("python3", ["-c", code], { encoding: "utf8" });
  if (r.status !== 0) throw new Error(`python fixture failed:\n${r.stderr}`);
  return r.stdout.trim();
};

/**
 * An N-page PDF whose pages are visually DISTINCT — each carries a filled bar whose vertical
 * position encodes its page number. Identical blank pages would make "page 3 is untouched"
 * unfalsifiable: every page would render to the same bytes and any mix-up would still pass.
 */
const makeBook = (file, pages, [w, h]) =>
  py(`
import pikepdf
pdf = pikepdf.Pdf.new()
for i in range(${pages}):
    page = pdf.add_blank_page(page_size=(${w}, ${h}))
    y = ${h} - 80 - (i * 13) % (${h} - 200)
    ops = f"0 0 0 rg 40 {y:.1f} {${w} - 80:.1f} 24 re f".encode()
    page.contents_add(pikepdf.Stream(pdf, ops))
pdf.save(${JSON.stringify(file)})
print("ok")
`);

const splice = (args) =>
  spawnSync("python3", [TOOL, ...args], { encoding: "utf8", cwd: process.cwd() });

const sha = (f) => crypto.createHash("sha256").update(fs.readFileSync(f)).digest("hex");
const pageCount = (f) => {
  const out = spawnSync("pdfinfo", [f], { encoding: "utf8" }).stdout;
  return Number(/^Pages:\s+(\d+)$/m.exec(out)[1]);
};
const pageSizes = (f) => {
  const n = pageCount(f);
  const out = spawnSync("pdfinfo", ["-f", "1", "-l", String(n), f], { encoding: "utf8" }).stdout;
  return [...out.matchAll(/^Page\s+(\d+) size:\s+([\d.]+) x ([\d.]+)/gm)].map((m) => [
    Number(m[1]), Math.round(Number(m[2])), Math.round(Number(m[3])),
  ]);
};
const withRender = (f, page, fn) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "splice-render-"));
  const prefix = path.join(dir, "p");
  spawnSync("pdftoppm", ["-f", String(page), "-l", String(page), "-r", "72", "-png",
    "-singlefile", f, prefix]);
  try {
    return fn(`${prefix}.png`);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
};

/** sha256 of one page's RENDER — what a device actually sees, not what the file happens to contain. */
const renderSha = (f, page) => withRender(f, page, sha);

/**
 * Is every pixel on all four edges of the rendered page white?
 *
 * This is "nothing was cropped off the page", and it is the only property that separates
 * fit-inside (correct) from fill-and-clip: BOTH scale uniformly, so ink proportions are
 * preserved either way and comparing aspect ratios proves nothing. Only the overflow shows.
 */
const hasWhiteBorder = (f, page) => withRender(f, page, (png) => {
  const out = py(`
from PIL import Image
im = Image.open(${JSON.stringify(png)}).convert("L")
w, h = im.size
px = im.load()
edges = (
    [px[x, 0] for x in range(w)] + [px[x, h - 1] for x in range(w)]
    + [px[0, y] for y in range(h)] + [px[w - 1, y] for y in range(h)]
)
print("clean" if min(edges) > 250 else "touches-edge")
`);
  return out === "clean";
});

let dir;
const fixture = (pages = 3, size = [768, 1024], sheetSize = [540, 720]) => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), "splice-test-"));
  const book = path.join(dir, "book.pdf");
  const sheet = path.join(dir, "sheet.pdf");
  makeBook(book, pages, size);
  makeBook(sheet, 1, sheetSize); // default: the shape PowerPoint actually exports
  return { book, sheet };
};

test.afterEach(() => {
  if (dir) fs.rmSync(dir, { recursive: true, force: true });
  dir = null;
});

test("appending grows the book by exactly one page", { skip: SKIP }, () => {
  const { book, sheet } = fixture(3);
  const r = splice(["--book", book, "--page", "4", sheet, "--expect-pages", "4"]);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(pageCount(book), 4);
});

test("a replaced page keeps the page COUNT unchanged", { skip: SKIP }, () => {
  const { book, sheet } = fixture(3);
  const r = splice(["--book", book, "--page", "2", sheet, "--expect-pages", "3"]);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(pageCount(book), 3);
});

test("an incoming 540x720 sheet is normalised to the book's page box", { skip: SKIP }, () => {
  // web/build.mjs renders at a FIXED DPI, so a page left at 540x720 comes out ~30% narrower in
  // pixels than its neighbours and is visibly softer on the same iPad. Page 371 shipped that way
  // in v425; this is the regression test for it.
  const { book, sheet } = fixture(3);
  assert.equal(pageCount(sheet), 1);
  assert.deepEqual(pageSizes(sheet), [[1, 540, 720]]);

  const r = splice(["--book", book, "--page", "4", sheet]);
  assert.equal(r.status, 0, r.stderr);
  const sizes = pageSizes(book);
  assert.equal(sizes.length, 4);
  for (const [n, w, h] of sizes) {
    assert.deepEqual([w, h], [768, 1024], `page ${n} is ${w}x${h}, not the book's page box`);
  }
});

test("pages the splice did not name render byte-for-byte identically", { skip: SKIP }, () => {
  // THE additive invariant, measured rather than assumed: every stale offline copy stays valid
  // only if untouched pages produce the same pixels. Build 377 / PR #257 re-rendered ~290 pages in
  // place and nothing noticed, because the page COUNT was right.
  const { book, sheet } = fixture(4);
  const before = [1, 2, 3, 4].map((p) => renderSha(book, p));

  const r = splice(["--book", book, "--page", "3", sheet, "--expect-pages", "4"]);
  assert.equal(r.status, 0, r.stderr);

  const after = [1, 2, 3, 4].map((p) => renderSha(book, p));
  for (const p of [1, 2, 4]) {
    assert.equal(after[p - 1], before[p - 1], `page ${p} changed but was never named`);
  }
  assert.notEqual(after[2], before[2], "page 3 was supposed to be replaced and was not");
});

test("a gap is refused, and the book is left untouched", { skip: SKIP }, () => {
  // Appending page 6 to a 3-page book would create pages 4 and 5 as blanks — which is exactly
  // what a singer typing "372" lands on when the book was supposed to stop at 372.
  const { book, sheet } = fixture(3);
  const before = sha(book);
  const r = splice(["--book", book, "--page", "6", sheet]);
  assert.equal(r.status, 1);
  assert.match(r.stderr, /gap/i);
  assert.equal(sha(book), before, "the book was modified by a call that failed");
});

test("--expect-pages is enforced BEFORE anything is written", { skip: SKIP }, () => {
  const { book, sheet } = fixture(3);
  const before = sha(book);
  const r = splice(["--book", book, "--page", "4", sheet, "--expect-pages", "9"]);
  assert.equal(r.status, 1);
  assert.match(r.stderr, /expect-pages/);
  assert.equal(sha(book), before, "the book was modified by a call that failed");
});

test("--dry-run writes nothing", { skip: SKIP }, () => {
  const { book, sheet } = fixture(3);
  const before = sha(book);
  const r = splice(["--book", book, "--page", "4", sheet, "--dry-run"]);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(sha(book), before);
  assert.equal(pageCount(book), 3);
});

test("the same page number twice is refused", { skip: SKIP }, () => {
  const { book, sheet } = fixture(3);
  const r = splice(["--book", book, "--page", "2", sheet, "--page", "2", sheet]);
  assert.equal(r.status, 1);
  assert.match(r.stderr, /twice/i);
});

test("a spliced page has INK when rendered by poppler", { skip: SKIP }, () => {
  // The fixture sheet has an empty content stream in its /Contents array (add_blank_page makes
  // one). Copied into the book, that member can be serialised as a zero-byte /FlateDecode stream
  // — not a zlib stream — and poppler abandons the whole page: BLANK on every iPad, green in every
  // count-based gate. Ghostscript renders it fine, which is why "it opens in Preview" proves
  // nothing. This is the test that would have caught it, so it renders with pdftoppm on purpose.
  const { book, sheet } = fixture(3);
  const r = splice(["--book", book, "--page", "4", sheet]);
  assert.equal(r.status, 0, r.stderr);
  const bbox = withRender(book, 4, (png) => py(`
from PIL import Image
im = Image.open(${JSON.stringify(png)}).convert("L")
print(Image.eval(im, lambda p: 255 - p).getbbox())
`));
  assert.notEqual(bbox, "None", "page 4 renders BLANK under poppler — the sheet's ink was lost");
});

test("a sheet of the wrong SHAPE is letterboxed, never cropped", { skip: SKIP }, () => {
  // A landscape sheet into the portrait book. Fitting inside (correct) and filling the box both
  // scale UNIFORMLY, so the ink keeps its proportions either way — the difference is only whether
  // the overflow runs off the page. Cropping a song sheet silently eats chords at the margins.
  const { book, sheet } = fixture(3, [768, 1024], [720, 540]);
  const r = splice(["--book", book, "--page", "4", sheet]);
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(pageSizes(book).at(-1), [4, 768, 1024]);
  assert.ok(
    hasWhiteBorder(book, 4),
    "the spliced sheet runs off the edge of the page — it was scaled to FILL, not to FIT",
  );
});

test("a missing source sheet fails cleanly, before the book is opened", { skip: SKIP }, () => {
  const { book } = fixture(3);
  const before = sha(book);
  const r = splice(["--book", book, "--page", "4", path.join(dir, "nope.pdf")]);
  assert.equal(r.status, 1);
  // A raw traceback exits 1 and leaves the book alone too, so status alone cannot tell the
  // difference between a checked path and an unhandled crash. Require the operator-facing message.
  assert.match(r.stderr, /no such file/i);
  assert.doesNotMatch(r.stderr, /Traceback/, "it crashed instead of reporting the bad path");
  assert.equal(sha(book), before);
});

test("--src-page out of range is refused", { skip: SKIP }, () => {
  const { book, sheet } = fixture(3);
  const before = sha(book);
  const r = splice(["--book", book, "--page", "4", sheet, "--src-page", "7"]);
  assert.equal(r.status, 1);
  assert.match(r.stderr, /out of range/);
  assert.equal(sha(book), before);
});

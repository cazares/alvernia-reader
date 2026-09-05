// A stale arm pointer put every CURRENT device into a never-settling re-stage loop.
//
// 479 shipped the way a release that skips the worker redeploy ships: the worker kept naming 471's
// book (BOOK_UPDATE_VERSION=bv_2d92…, DEVICES "*"). A device on the current book reports a different
// bookVersion, is offered the stale one, fetches bundle-manifest.json?v=<stale>, the origin serves the
// CURRENT manifest, and stageBook fails "version-mismatch". Only "cannot-outrank-baked-shell" counted
// toward quarantine, so the identical attempt repeated on every 4-minute check-in and every foreground
// for the life of the install: the fleet check-in showed the CURRENT devices as the failing ones, and
// the 200-slot breadcrumb ring (the only Mass forensics) filled with stage-failed lines. Confirmed by
// two independent skeptics at fa5b8bd (sunday-readiness upgrade-path-1).
//
// The fix counts "version-mismatch" toward the SAME quarantine counter (3 strikes, keyed by bookVersion,
// cleared by a proven boot of that version), so the loop settles in ~12 minutes and a genuinely new book
// — a new hash — is never affected. Re-injected by scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APP = readFileSync(join(ROOT, "PdfReaderApp.tsx"), "utf8");
const BOOK_UPDATE = readFileSync(join(ROOT, "src", "bookUpdate.js"), "utf8");

// The quarantine block: from the stage-failed breadcrumb to the shell-too-old notice that follows it.
const quarantineBlock = () => {
  const start = APP.indexOf("breadcrumb(rec.ready ? `staged-ready:${rec.bookVersion}` : `stage-failed:${rec.error}`);");
  assert.notEqual(start, -1, "the stage-failed breadcrumb moved");
  const end = APP.indexOf("A binary too old to run this book", start);
  assert.notEqual(end, -1, "the shell-too-old notice that follows the quarantine block moved");
  return APP.slice(start, end);
};

test("a version-mismatch stage failure counts toward quarantine, not just cannot-outrank-baked-shell", () => {
  const block = quarantineBlock();
  const cond = block.match(/if \(\s*\(([^)]*)\)\s*&&\s*rec\.bookVersion\s*\)/);
  assert.ok(cond, "the quarantine condition is no longer '(<errors>) && rec.bookVersion' — a stale pointer's manifest fetch repeats forever");
  assert.match(cond[1], /rec\.error === "cannot-outrank-baked-shell"/, "cannot-outrank-baked-shell dropped out of the quarantine condition");
  assert.match(cond[1], /rec\.error === "version-mismatch"/,
    "version-mismatch does not count toward quarantine — a stale arm pointer re-fetches the manifest on every check-in and every foreground for the life of the install");
  assert.match(block, /recordBundleFailure\(/, "the block no longer records the failure in the quarantine counter");
});

test("the error string the shell quarantines is the one stageBook actually emits", () => {
  // A rename on either side would silently disconnect the two; pin both spellings to each other.
  assert.match(BOOK_UPDATE, /fail\("version-mismatch"[,)]/, "stageBook no longer fails with the literal 'version-mismatch'");
  assert.match(BOOK_UPDATE, /fail\("cannot-outrank-baked-shell"[,)]/, "stageBook no longer fails with the literal 'cannot-outrank-baked-shell'");
});

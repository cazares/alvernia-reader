// A director's first broadcast was the web's INTENDED page, not the page on their SCREEN.
//
// On a native boot with nobody directing yet — the director's own iPad at the start of every Mass —
// initReader sets state.currentPage = DEFAULT_START_PAGE (2) WITHOUT rendering, posts bridge-ready
// {page: 2} (native's follower branch adopts it into its mirror), waits up to 2.5 s for a native page
// that never comes, and reveals the reader on index.html's static page-001 (the cover). Nothing
// reconciled the two: the screen said 1, state and native's mirror said 2. "Ser Director" sends
// state.currentPage as the page to broadcast, so the whole choir was sent to song 2 while the
// director looked at the cover, and the director's first swipe went cover → song 3 (clampPage(2 + 1)).
// Reproduced on two isolated simulators on 2026-09-04: director on the cover, follower on
// "2. Bendito, Bendito", relay holding page 2.
//
// The fix: after the reveal gate, if no native page arrived and the screen disagrees with state,
// RENDER the page that is on screen. renderPage re-aligns state, posts page-changed (native's mirror
// follows) and re-syncs the badge — before anyone can act on any of them. A native page that arrives
// later still wins: it bumps the load request and this render steps aside.
//
// Each defect below is re-injected by scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APP = readFileSync(join(ROOT, "web", "src", "app.js"), "utf8");

// Lift renderedPage() out of app.js and EXECUTE it against a fake <img> — the assertion is on
// behaviour, not on the presence of a comment.
const liftRenderedPage = () => {
  const start = APP.indexOf("const renderedPage = () => {");
  assert.notEqual(start, -1, "renderedPage() is missing from app.js");
  const end = APP.indexOf("\n};", start);
  assert.notEqual(end, -1, "renderedPage() has no closing brace");
  const body = APP.slice(start, end + 3);
  return (pageImage, state) =>
    new Function("pageImage", "state", `${body}\nreturn renderedPage();`)(pageImage, state);
};

const img = (src, dataPage) => ({
  dataset: dataPage === undefined ? {} : { page: dataPage },
  getAttribute: (name) => (name === "src" ? src : null),
});

test("renderedPage() reads the page from the <img> itself — the static boot image is page 1", () => {
  const renderedPage = liftRenderedPage();
  const state = { currentPage: 2 };
  assert.equal(renderedPage(img("books/standard/pages/page-001.webp"), state), 1,
    "index.html's static page-001 must read as page 1 — state says 2, and state is what was wrong");
  assert.equal(renderedPage(img("file:///x/books/standard/pages/page-017.webp", "17"), state), 17,
    "a rendered page is stamped in data-page and must win");
  assert.equal(renderedPage(img("books/standard/pages/page-345.webp?r=1"), state), 345,
    "a retry token on the src must not hide the page number");
  assert.equal(renderedPage(img("", undefined), state), 2,
    "with nothing on the <img>, fall back to state — never to a made-up page");
});

test("after the native reveal gate, a screen that disagrees with state is RENDERED so state, mirror and badge agree", () => {
  const race = APP.indexOf(
    "firstNativePageSignal,\n        new Promise((resolve) => setTimeout(resolve, FIRST_NATIVE_PAGE_TIMEOUT_MS))",
  );
  assert.notEqual(race, -1, "the reveal-gate race moved");
  const reveal = APP.indexOf("revealReader();", race);
  assert.notEqual(reveal, -1, "the revealReader() call moved");
  const between = APP.slice(race, reveal);
  assert.match(between, /if \(!firstNativePageArrived && renderedPage\(\) !== state\.currentPage\)/,
    "nothing reconciles state.currentPage with the page on screen when no native page arrived — the boot default is broadcast while the cover is on screen");
  assert.match(between, /await renderPage\(renderedPage\(\), \{ pushToHistory: false \}\)/,
    "the reconciliation must RENDER the on-screen page (renderPage posts page-changed, which corrects native's mirror), not merely assign state");
});

test("the sync-event page handler records that a native page arrived, so a real page is never overridden by the reconciliation", () => {
  const at = APP.indexOf('if (event.type === "page" && Number.isFinite(event.page)) {');
  assert.notEqual(at, -1, "the native page handler moved");
  const stop = APP.indexOf("resolveFirstNativePageSignal = null;", at);
  assert.notEqual(stop, -1, "the reveal-gate resolver moved");
  const handler = APP.slice(at, stop);
  assert.match(handler, /^\s*firstNativePageArrived = true;/m,
    "the handler does not record the arrival — the reconciliation cannot tell a real page from the static boot image");
  assert.match(APP, /^let firstNativePageArrived = false;/m,
    "firstNativePageArrived is not declared at module scope");
});

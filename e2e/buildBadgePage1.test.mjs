import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");

// The badge answers "which build and which book is this device holding?" — asked once while
// checking a device, not something anyone needs floating over the music for 372 pages.

// Execute the real visibility rule against a fake element, so a changed comparison changes the
// RESULT rather than just moving a line some assertion still matches.
const visibleOn = (page) => {
  const src = APP.slice(APP.indexOf("function syncBuildBadgeVisibility"),
                        APP.indexOf("const renderDirectorModeBadge"));
  let shown = null;
  const el = { classList: { toggle: (_c, v) => { shown = v; } } };
  new Function("document", "state", "BUILD_BADGE_PAGE",
    `${src}; syncBuildBadgeVisibility();`,
  )({ getElementById: () => el }, { currentPage: page },
    Number(APP.match(/const BUILD_BADGE_PAGE = (\d+)/)[1]));
  return shown;
};

test("the badge shows on page 1 and nowhere else", () => {
  assert.equal(visibleOn(1), true, "the badge is hidden on page 1, where it is the whole point");
  for (const p of [2, 3, 42, 200, 373]) {
    assert.equal(visibleOn(p), false, `the badge is still showing on page ${p}`);
  }
});

test("visibility is re-evaluated whenever the page settles", () => {
  // Set once at boot and never again, the badge would stick on whatever the first page was.
  // Bounded by the next statement in the commit sequence rather than a character count — a comment
  // added between the two silently slid the call out of a 200-char window and reddened a test about
  // behaviour that was entirely intact.
  const start = APP.indexOf("state.currentPage = nextPage;");
  const end = APP.indexOf("pageImage.dataset.page", start);
  assert.ok(start > 0 && end > start, "the render commit sequence moved — re-bound this test");
  assert.match(APP.slice(start, end), /syncBuildBadgeVisibility\(\)/,
    "the page-change path does not re-evaluate the badge");
});

test("the initial paint respects the rule instead of forcing the badge on", () => {
  const init = APP.slice(APP.indexOf("const buildBadge = document.getElementById"));
  assert.ok(!/buildBadge\.classList\.add\("is-shown"\)/.test(init.slice(0, 600)),
    "boot still force-shows the badge, so it appears on whatever page the app opens to");
});

test("the helper is hoisted, because its caller is thousands of lines earlier", () => {
  // A `const` arrow would sit in the temporal dead zone if anything ever calls renderPage at module
  // top level. Nothing does today; this keeps that from becoming a trap.
  assert.match(APP, /function syncBuildBadgeVisibility\(\)/, "not a hoisted function declaration");
  assert.ok(APP.indexOf("syncBuildBadgeVisibility()") > APP.indexOf("function syncBuildBadgeVisibility"),
    "the definition must precede its first call in source order");
});

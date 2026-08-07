import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";

const APP = fs.readFileSync("web/src/app.js", "utf8");

// A director's mesh heartbeat arrives once per SECOND. Every one of them called renderPage, even
// when the page had not moved — bumping the load request, scheduling a loading indicator,
// reassigning pageImage.src, and posting `page-changed` to native, which writes AsyncStorage. Sixty
// times a minute for the length of a Mass, on an eight-year-old iPad. That is the crash reported on
// 2026-08-06: a follower left on one screen for several minutes, doing nothing.

// Execute the real guard so a weakened condition changes the RESULT, not just a line of source.
const skips = ({ next, current, imgMatches, complete, naturalWidth }) => {
  const start = APP.indexOf("const renderPage = async");
  const guard = APP.slice(start, APP.indexOf("const requestId = state.pageLoadRequest + 1;", start));
  const body = guard.slice(guard.indexOf("if ("));
  const fn = new Function("nextPage", "state", "pageImage", "pageImageMatches",
    `${body.replace(/\n\s*return;\n\s*\}/, "\n    return true;\n  }")}\n  return false;`);
  return fn(next, { currentPage: current }, { complete, naturalWidth }, () => imgMatches);
};
const base = { next: 50, current: 50, imgMatches: true, complete: true, naturalWidth: 800 };

test("an idle heartbeat on the same page does no work", () => {
  assert.equal(skips(base), true, "the follower re-renders on every heartbeat — this is the crash");
});

test("a real page turn still renders", () => {
  assert.equal(skips({ ...base, next: 51 }), false, "page turns would stop working");
});

test("a page that is not actually on screen still renders", () => {
  // The guard must never strand a follower on a blank while the mesh insists the page is current.
  assert.equal(skips({ ...base, complete: false }), false, "a pending load was skipped");
  assert.equal(skips({ ...base, naturalWidth: 0 }), false, "a FAILED load was skipped — blank forever");
  assert.equal(skips({ ...base, imgMatches: false }), false, "the wrong image was left on screen");
});

test("the guard sits before any work is scheduled", () => {
  // Placed after the request bump or the loading indicator, it would save nothing.
  const start = APP.indexOf("const renderPage = async");
  const head = APP.slice(start, APP.indexOf("const requestId = state.pageLoadRequest + 1;", start));
  assert.ok(!/scheduleLoadingIndicator/.test(head), "the loading indicator is scheduled before the guard");
  assert.match(head, /return;/, "the guard does not actually return early");
});

test("the director search panel is build 381's geometry", () => {
  // 381 is the last known-good prod version. The 1.75rem ring is load-bearing: #drawer-backdrop
  // shows through it and tapping it closes search, so a thinner ring is a harder exit target.
  const CSS = fs.readFileSync("web/src/styles.css", "utf8");
  const block = CSS.slice(CSS.indexOf(".navigation-drawer.as-dropdown.search-fullscreen {"),
                          CSS.indexOf("/* Hide the full-drawer chrome"));
  for (const side of ["top", "bottom", "left", "right"]) {
    assert.match(block, new RegExp(`${side}: max\\(1\\.75rem`), `${side} is not 381's tappable ring`);
  }
  assert.match(block, /width: auto !important/, "the panel is width-constrained again");
  assert.match(block, /height: auto !important/, "the panel is height-constrained again");
  assert.match(CSS, /@keyframes sv-search-drop/, "381's slide-down keyframe is missing");
});

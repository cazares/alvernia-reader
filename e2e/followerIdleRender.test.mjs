import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
// renderPage's preamble now also consults the render PACING rule (lib/svRenderPace.js), which
// paces retries of a page that keeps failing to load. This file evaluates that preamble as real
// code, so it has to supply the real dependency — a stub would let this file pass while the two
// guards disagreed in the browser. With no recorded failure the pacing rule always returns false,
// so every assertion below still isolates the SAME-PAGE guard, which is what it is here to test.
const { shouldPaceRender } = require("../web/src/lib/svRenderPace.js");

const APP = fs.readFileSync("web/src/app.js", "utf8");

// A director's mesh heartbeat arrives once per SECOND. Every one of them called renderPage, even
// when the page had not moved — bumping the load request, scheduling a loading indicator,
// reassigning pageImage.src, and posting `page-changed` to native, which writes AsyncStorage. Sixty
// times a minute for the length of a Mass, on an eight-year-old iPad. That is the crash reported on
// 2026-08-06: a follower left on one screen for several minutes, doing nothing.

// Execute the real guard so a weakened condition changes the RESULT, not just a line of source.
const skips = ({ next, current, imgMatches, complete, naturalWidth, userInitiated = false }) => {
  const start = APP.indexOf("const renderPage = async");
  const guard = APP.slice(start, APP.indexOf("const requestId = state.pageLoadRequest + 1;", start));
  const body = guard.slice(guard.indexOf("if ("));
  const fn = new Function("nextPage", "state", "pageImage", "pageImageMatches", "svShouldPaceRender", "userInitiated",
    `${body.replace(/\n\s*return;\n\s*\}/, "\n    return true;\n  }")}\n  return false;`);
  // lastRenderFailure: null — no page is currently failing, so the pacing rule is inert here.
  return fn(next, { currentPage: current, lastRenderFailure: null }, { complete, naturalWidth }, () => imgMatches, shouldPaceRender, userInitiated);
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

// ── Pacing must never swallow a HUMAN page turn ───────────────────────────────
//
// svRenderPace blunts the director's 1 Hz re-drive of a page that cannot render. It lives inside
// renderPage, so it was also catching SWIPES: a page that failed once became unreachable by hand
// for five seconds, silently — no error, no feedback, the swipe just did nothing.
//
// Reported on hardware 2026-08-17: "I was on song 3 or 4 and it wouldn't let me swipe to songs 4 or
// 5... going back and returning then retrying fixed the issue." That is the window expiring.
//
// Executed, not quoted: this drives the real guard with a live failure memo.
const skipsWithFailure = ({ next, userInitiated }) => {
  const start = APP.indexOf("const renderPage = async");
  const guard = APP.slice(start, APP.indexOf("const requestId = state.pageLoadRequest + 1;", start));
  const body = guard.slice(guard.indexOf("if ("));
  const fn = new Function("nextPage", "state", "pageImage", "pageImageMatches", "svShouldPaceRender", "userInitiated",
    `${body.replace(/return;/g, "return true;")}\n  return false;`);
  // This page failed 100ms ago — squarely inside the 5s pacing window.
  return fn(next, { currentPage: 3, lastRenderFailure: { page: next, at: Date.now() - 100 } },
            { complete: false, naturalWidth: 0 }, () => false, shouldPaceRender, userInitiated);
};

test("the director's automatic re-drive of a failing page IS paced", () => {
  assert.equal(skipsWithFailure({ next: 4, userInitiated: false }), true,
    "without pacing, a page that cannot render loops at the heartbeat rate — the 2026-08-06 crash");
});

test("a human swipe to that same page is NOT paced", () => {
  assert.equal(skipsWithFailure({ next: 4, userInitiated: true }), false,
    "a swipe was silently swallowed: the user asked once and got nothing, not even an error");
});

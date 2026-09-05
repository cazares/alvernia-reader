// A director's mis-tap correction left the follower on the WRONG page until the next re-drive.
//
// renderPage's "ALREADY ON THIS PAGE" early return (added to stop the 1 Hz re-render storm) checks the
// page against state.currentPage and the <img>, but not against a render still IN FLIGHT for a
// different page. Director on B taps to A (the follower starts loading A; state.currentPage is still B
// and the <img> still shows B), then taps back to B within A's load time: renderPage(B) hits the early
// return and does nothing — so A's load completes and COMMITS A over a correct B. The follower then
// sits on A until the director's next heartbeat happens to re-drive B against a now-different
// state.currentPage: ≤1 s over the mesh, up to ~4 s for a relay (web) follower. Confirmed by two
// independent skeptics at fa5b8bd (web-follower-4). The same early return also kept a stale
// "No se pudo cargar esta página" overlay up for a page that was no longer being asked for.
//
// The fix: the page on screen wins over a page in flight. The early return invalidates any pending
// request (state.pageLoadRequest is read back after every await, so the in-flight render steps aside)
// and clears the indicator that render scheduled. Re-injected by scripts/verify-behavioural-guards.mjs.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const APP = readFileSync(join(ROOT, "web", "src", "app.js"), "utf8");

// The early-return block: from its condition to the `return;` that ends it.
const earlyReturn = () => {
  const cond = APP.indexOf("pageImage.naturalWidth > 0\n  ) {");
  assert.notEqual(cond, -1, "renderPage's already-on-this-page early return moved");
  const ret = APP.indexOf("\n    return;", cond);
  assert.notEqual(ret, -1, "the early return no longer returns");
  return APP.slice(cond, ret + "\n    return;".length);
};

test("a request for the page already on screen invalidates a render still in flight for another page", () => {
  const block = earlyReturn();
  assert.match(block, /state\.pageLoadRequest \+= 1;/,
    "the early return does not bump pageLoadRequest — an in-flight render of a DIFFERENT page commits over the correct one after a mis-tap correction");
  assert.match(block, /hideLoadingIndicator\(\);/,
    "the early return leaves the in-flight render's loading indicator (or a stale error overlay) on screen after cancelling it");
});

test("pageLoadRequest is still the guard every in-flight render checks after each await", () => {
  // If renderPage stopped comparing requestId against state.pageLoadRequest after its awaits, the bump
  // above would cancel nothing — pin the mechanism the fix relies on.
  const checks = APP.match(/if \(requestId !== state\.pageLoadRequest\) return;/g) || [];
  assert.ok(checks.length >= 2, `renderPage checks requestId after its awaits ${checks.length} time(s); expected at least 2 (post-load and in the catch)`);
});

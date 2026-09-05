// A device that has stopped directing must stop PUBLISHING — on every path, not just the polite one.
//
// setRelayPublishing(true) is called at the top of becomeDirector, before any broadcast. The relay
// transmitter (src/directorRelaySync.js) coalesces page frames and drains its queue asynchronously, so
// "we are no longer the director" and "the last queued frame has left" are different moments. becomeFollower
// disables publishing (the DIRECTOR_CONFLICT step-down goes through it), but two other exits from the
// director role did not:
//
//   1. performSoftReset — the operator's recovery code. It stopped the heartbeat and reset the mesh, set
//      roleRef "off"… and left publishing ENABLED, so a frame still in the coalescer drained to the relay
//      from a device that had just reset. Every signovivo.com follower flipped to that page.
//   2. becomeDirector's failure path when the device was NOT previously a follower: it injected role
//      "none" and returned, with publishing still enabled from the top of the same call.
//
// And performSoftReset left the device in role "off" with mesh, BLE and relay all torn down, while the
// remounted WebView came up showing the ordinary follower UI — a screen that says "following" on a device
// that follows nothing, until a human taps the pill. Every other path back to a neutral state
// (bootstrap's `.finally(() => becomeFollower())`) re-enters follower mode; the reset now does too.
//
// Structural pins on a file that cannot be imported without a React Native runtime; each defect is
// re-injected by scripts/verify-behavioural-guards.mjs and the NAMED test must go red.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SRC = readFileSync(join(ROOT, "PdfReaderApp.tsx"), "utf8");

/** Brace-matched body starting at the first `{` after `from`. Never runs to EOF on a missing marker. */
function bodyAt(from, what) {
  const open = SRC.indexOf("{", from);
  assert.notEqual(open, -1, `no body found for ${what}`);
  let depth = 0;
  for (let i = open; i < SRC.length; i++) {
    if (SRC[i] === "{") depth++;
    else if (SRC[i] === "}") { depth--; if (depth === 0) return SRC.slice(open, i + 1); }
  }
  assert.fail(`unbalanced braces reading ${what}`);
}

test("performSoftReset disables relay publishing before it tears the role down", () => {
  const at = SRC.indexOf("const performSoftReset = useCallback(");
  assert.notEqual(at, -1, "performSoftReset must exist");
  const body = bodyAt(at, "performSoftReset");
  const off = body.indexOf("setRelayPublishing(false)");
  assert.notEqual(off, -1, "a soft reset leaves relay publishing ENABLED — a queued frame drains to every web follower from a device that just reset");
  const roleOff = body.indexOf('roleRef.current = "off"');
  assert.ok(roleOff !== -1 && off < roleOff, "publishing must be disabled BEFORE the role flips off, or the window is open");
});

test("performSoftReset re-enters follower mode after the remount instead of stranding the device 'off'", () => {
  const at = SRC.indexOf("const performSoftReset = useCallback(");
  const body = bodyAt(at, "performSoftReset");
  const remount = body.indexOf("setMountKey((k) => k + 1)");
  assert.notEqual(remount, -1, "the reset must still remount the WebView");
  // Match the CALL STATEMENT, not the text: a comment in this very function mentions `becomeFollower()`
  // and made the first version of this assertion pass with the call deleted (caught by the harness).
  const afterRemount = body.slice(remount);
  assert.match(afterRemount, /^\s*void becomeFollower\(\);/m,
    "after a soft reset the device stays in role 'off' with every transport down while the web shows the follower UI");
});

test("becomeDirector's non-follower failure path disables relay publishing", () => {
  const at = SRC.indexOf("const becomeDirector = useCallback(");
  assert.notEqual(at, -1, "becomeDirector must exist");
  const body = bodyAt(at, "becomeDirector");
  // The failure branch that does NOT go through becomeFollower: it only injects role "none".
  const none = body.indexOf('injectEvent({ type: "role", role: "none" });');
  assert.notEqual(none, -1, "the role-none failure branch must still exist");
  // Look only at the else-branch containing that inject: from the `} else {` before it to the `}` after it.
  const elseAt = body.lastIndexOf("} else {", none);
  const close = body.indexOf("}", none);
  assert.ok(elseAt !== -1 && close !== -1, "could not bound the role-none branch");
  assert.match(body.slice(elseAt, close), /setRelayPublishing\(false\)/,
    "a failed takeover that was not previously following leaves publishing enabled with no director role");
});

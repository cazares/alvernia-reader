// A director must never broadcast a page it does not actually have.
//
// The follower render-failed sentinel writes currentPageRef = -1. Three separate defects let that
// -1 reach the whole fleet AS PAGE 1 — a confident wrong page, sent with the same authority as a
// real page turn, that did not self-correct and that the director could not see (a director never
// receives their own broadcast, so their screen still showed the right page):
//
//   1. broadcastPage FLOORED -1 to 1. Its own comment said it existed to stop "yanking the whole
//      congregation" to page 1 — which is precisely what flooring to 1 does.
//   2. The 100 ms director heartbeat did not go through broadcastPage at all, so the "single choke
//      point that feeds BOTH transports" was never single. It handed -1 to Swift, which clamps to 1.
//   3. render-failed's role gate reads roleRef === "follower", but roleRef STAYS "follower" for the
//      whole of becomeDirector's await window — so the sentinel overwrote the mirror correction
//      becomeDirector had just made, at the exact moment a stuck follower takes the role.
//
// These are structural pins on a file that cannot be imported without a React Native runtime. They
// are only worth anything because scripts/verify-behavioural-guards.mjs re-injects each defect and
// requires the NAMED test below to go red. If you change these, re-run that harness.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SRC = readFileSync(join(ROOT, "PdfReaderApp.tsx"), "utf8");

// Extract a balanced brace-delimited body starting at `from`. Never runs to EOF on a missing
// marker (the failure shape that made five files in this repo assert nothing).
function bodyAt(from, what) {
  const open = SRC.indexOf("{", from);
  assert.notEqual(open, -1, `no body found for ${what}`);
  let depth = 0;
  for (let i = open; i < SRC.length; i++) {
    if (SRC[i] === "{") depth++;
    else if (SRC[i] === "}") {
      depth--;
      if (depth === 0) return SRC.slice(open, i + 1);
    }
  }
  assert.fail(`unbalanced braces reading ${what}`);
}

test("broadcastPage refuses an invalid page instead of flooring it to page 1", () => {
  const at = SRC.indexOf("const broadcastPage = useCallback(");
  assert.notEqual(at, -1, "broadcastPage must exist");
  const body = bodyAt(at, "broadcastPage");

  assert.doesNotMatch(
    body,
    /rawPage\s*>=\s*1\s*\?\s*rawPage\s*:\s*1/,
    "flooring an invalid page to 1 sends the congregation to page 1 — refuse instead",
  );
  assert.match(
    body,
    /if\s*\(\s*!Number\.isFinite\(rawPage\)\s*\|\|\s*rawPage\s*<\s*1\s*\)/,
    "broadcastPage must guard a non-positive page",
  );
  // The guard is worthless unless it actually leaves the function.
  const guardAt = body.search(/if\s*\(\s*!Number\.isFinite\(rawPage\)/);
  const sendAt = body.indexOf("sendNearbyDirectorPageUpdate");
  assert.ok(guardAt !== -1 && sendAt !== -1 && guardAt < sendAt, "the guard must precede the send");
  assert.match(body.slice(guardAt, sendAt), /\breturn\b/, "the guard must return, not fall through");
});

test("the director heartbeat refuses an invalid page before sending it", () => {
  const at = SRC.indexOf("const startDirectorHeartbeat = useCallback(");
  assert.notEqual(at, -1, "startDirectorHeartbeat must exist");
  const body = bodyAt(at, "startDirectorHeartbeat");

  const sendAt = body.indexOf("sendNearbyDirectorPageUpdate(currentPageRef.current");
  assert.notEqual(sendAt, -1, "the heartbeat must still send the mesh page update");
  const guardAt = body.search(/if\s*\(\s*!Number\.isFinite\(currentPageRef\.current\)\s*\|\|\s*currentPageRef\.current\s*<\s*1\s*\)\s*return;/);
  assert.ok(guardAt !== -1, "the heartbeat needs its own guard — it bypasses broadcastPage entirely");
  assert.ok(guardAt < sendAt, "the guard must run before the send, or it guards nothing");
});

test("render-failed does not blank the mirror while this device is becoming director", () => {
  const at = SRC.indexOf('currentPageRef.current = -1;');
  assert.notEqual(at, -1, "the render-failed sentinel must still exist");
  // Look only at the guard immediately above the sentinel write — a bounded window by structure,
  // not by character count.
  const guardLine = SRC.lastIndexOf("if (roleRef.current ===", at);
  assert.ok(guardLine !== -1 && guardLine < at, "the sentinel write must be guarded");
  const guard = SRC.slice(guardLine, at);
  assert.match(
    guard,
    /!becomeDirectorInFlightRef\.current/,
    "roleRef stays 'follower' through becomeDirector's awaits, so the role check alone is not enough",
  );
});

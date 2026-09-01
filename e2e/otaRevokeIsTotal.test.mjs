import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

// A REVOKE MUST BEAT EVERY STATE A DEVICE CAN BE IN.
//
// Disarming is the operator's only way to stop a bad songbook once it is out, and the person who
// knows it is bad is not in the building. The relay expresses it as a 200 with no `bookUpdate`, and
// the client is supposed to treat that as "delete it, whatever you have."
//
// It did not. Three separate states slipped through, found in three different rounds of hunting,
// and each one ends the same way: that iPad renders a different edition than the rest of the fleet
// while the director turns pages. Pinned together here because they are one invariant, not three
// unrelated fixes, and the next one will be a fourth state nobody thought of.
//
//   1. STAGED       — handled from the start: the staged record is deleted.
//   2. PARKED       — the pointer ⟳ replays when offline was written and never cleared, so a tap in
//                     a bad-signal parking lot re-downloaded the withdrawn book. A revoke that one
//                     tap undoes is not a revoke.
//   3. DOWNLOADING  — a revoke arriving mid-flight matched nothing (the staged record does not
//                     exist until stageBook finishes), so it was dropped and the download ran to
//                     completion and auto-applied.
//
// And the shape of the response matters as much as the handling: an internal worker error must NOT
// be able to impersonate a revoke, or a transient hiccup destroys 27 MB of verified work.

const NATIVE = fs.readFileSync("PdfReaderApp.tsx", "utf8");
const WORKER = fs.readFileSync("sync-worker/src/index.ts", "utf8");

/** The `!pointer` branch of onCheckinResponse — everything a revoke does. */
const revokeBranch = () => {
  const start = NATIVE.indexOf("THE ABORT IS A REAL REVOKE");
  assert.ok(start > 0, "the revoke branch is gone");
  const end = NATIVE.indexOf("OWNER DECISION, 2026-08-05", start);
  assert.ok(end > start, "could not bound the revoke branch");
  return NATIVE.slice(start, end);
};

test("a revoke deletes a STAGED copy", () => {
  const b = revokeBranch();
  assert.match(b, /removeItem\(STORAGE_KEYS\.bookStaged\)/, "the staged record survives a revoke");
  assert.match(b, /rmrf\("WebBundleStaged"\)/, "the staged bytes survive a revoke");

  // …and the CONDITION that decides whether any of that runs. The two assertions above only prove
  // the deletions exist SOMEWHERE inside the branch; narrowing `!pointer ||` to `pointer &&` leaves
  // them untouched while making a revoke — the no-pointer case, which is the whole point —
  // unreachable. Measured: the full three-file suite stayed 27/27 green under exactly that mutation.
  assert.match(
    NATIVE,
    /if \(staged\?\.bookVersion && \(!pointer \|\| pointer\.bookVersion !== staged\.bookVersion\)\)/,
    "the revoke branch no longer triggers on a MISSING pointer — which is what a revoke IS",
  );
});

test("a revoke clears the PARKED pointer that ⟳ replays offline", () => {
  // pendingPointerRef is only read by refreshBookNow, and only when the tap's own check-in could not
  // reach the relay. Leaving a withdrawn pointer there meant one ⟳ tap on a bad network re-staged
  // the book the operator had just pulled — and nothing downstream objects, because the staged
  // record is already gone so the revoke branch no-ops on the replay.
  const b = revokeBranch();
  // Presence is not enough: wrapping it in `if (staged?.bookVersion)` still matches, and then a
  // device that was ARMED but never finished staging keeps the withdrawn pointer forever.
  assert.match(b, /^\s*pendingPointerRef\.current = null;\s*$/m,
    "the parked-pointer clear is conditional — a device that never staged keeps the withdrawn book");
});

test("a revoke stops a download already in flight", () => {
  const b = revokeBranch();
  assert.match(b, /if \(stagingInFlightRef\.current\) stagingDisarmedRef\.current = true/,
    "a revoke arriving mid-download is dropped — the device finishes and installs it anyway");

  // …and the staging completion must actually honour it, BEFORE applying.
  const tail = NATIVE.slice(NATIVE.indexOf('stagingInFlightRef.current = true;'));
  // ANCHOR ON THE CHECK, NOT THE RESET. This found `stagingDisarmedRef.current = false` — the
  // RESET, 43 chars in — so `disarmIdx < applyIdx` was trivially true wherever the real check
  // lived, and the whole test rested on the rmrf-between assertion, which any unrelated rmrf in
  // that span satisfies.
  const disarmIdx = tail.indexOf("if (stagingDisarmedRef.current)");
  const applyIdx = tail.indexOf("autoApplyIfSafeRef.current?.()");
  assert.ok(disarmIdx > 0 && applyIdx > 0, "the staging completion path moved");
  assert.ok(disarmIdx < applyIdx,
    "the disarm is checked AFTER the apply — the withdrawn book installs before anyone looks");
  assert.match(tail.slice(disarmIdx, applyIdx), /rmrf\("WebBundleStaged"\)/,
    "a disarmed download is kept on disk rather than discarded");
});

test("a fresh staging run starts un-disarmed", () => {
  // Otherwise one revoke would poison every future download on that device until relaunch.
  const start = NATIVE.indexOf("stagingInFlightRef.current = true;");
  const window = NATIVE.slice(start, start + 200);
  assert.match(window, /stagingDisarmedRef\.current = false/,
    "the disarm flag is never cleared — a later legitimate update is discarded too");
});

test("an internal worker error cannot impersonate a revoke", () => {
  // The destructive shape is 200-without-bookUpdate. A throw must never produce it, or a transient
  // Durable Object hiccup deletes a verified 27 MB copy on every armed device that checks in.
  // SLICE FROM THE ROUTE, NOT ITS FIRST MENTION. This started at the first occurrence of the bare
  // path string — the NON_ESSENTIAL expression far above the route — a window already containing
  // two other `503`s (a budget return, and the words "A 503 IS T…" inside a comment), so the bare
  // /503/ could never fail. Changing the route's own status to 200 — the destructive shape — left
  // this test green.
  const routeAt = WORKER.indexOf('if (url.pathname === "/ota/checkin")');
  assert.ok(routeAt > 0, "the /ota/checkin route moved");
  const route = WORKER.slice(routeAt, WORKER.indexOf('if (url.pathname === "/log")', routeAt));
  assert.ok(route.length > 0, "could not bound the /ota/checkin route");
  assert.match(route, /"arming_unavailable" \}, 503/,
    "an internal failure no longer returns 503 — a 200 without bookUpdate IS a revoke to every client");

  // …including from the outer catch-all, which returns a friendly 200 for every OTHER route.
  const outer = WORKER.slice(WORKER.indexOf("Last-resort guard for anything"));
  assert.match(outer, /url\.pathname === "\/ota\/checkin"/,
    "the outer catch still answers /ota/checkin with a 200 — the destructive shape is back");
});

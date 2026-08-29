import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

// THE M-F7 BACKOFF LADDER, WHICH HAD NEVER CLIMBED A SINGLE RUNG.
//
// DirectorSyncModule documents the intent plainly: "fast exponential backoff for the first 5
// failures … then a SLOW 45 s last-resort retry FOREVER — never give up permanently", and
// handleAppDidBecomeActive has a `> 5` branch that gives a device whose Local Network permission was
// just granted in Settings one more shot. Neither has ever run.
//
// Originally because the counter was reset on ATTEMPT, inside the same function the retry calls back
// into: it oscillated 0↔1 forever and a permission denial meant a 3 s teardown/rebuild loop for the
// whole Mass. Then, after that was fixed with a settle timer, because the settle scheduled by an
// attempt that ALREADY FAILED still fired during the wait once the delay grew past it — the count
// sawtoothed 1,2,3,reset,1,2,3,reset and stopped at the third rung.
//
// The arithmetic is replayed here rather than only grepped, because "the counter increments" and
// "the ladder actually reaches its last-resort tier" are different claims and only the second one
// matters.

const MODULE = fs.readFileSync("ios/SignoVivo/DirectorSyncModule.swift", "utf8");

// THE LADDER IS READ OUT OF THE SWIFT, NOT RESTATED HERE.
//
// This was a hand-copied JS constant fed only into the test's own model, so the entire M-F7 retry
// could have been deleted from DirectorSyncModule and every test in the repo would still have been
// green — a model of a ladder that no longer exists, proving a property of itself. The whole point
// of replaying the arithmetic instead of grepping for it is that "the ladder reaches its last-resort
// tier" is a claim about the shipped code.
const ladder = () => {
  const m = MODULE.match(
    /advertiserFailureCount <= (\d+)\s*\?\s*min\(([\d.]+) \* pow\(([\d.]+), Double\(self\.advertiserFailureCount - 1\)\), ([\d.]+)\)\s*:\s*([\d.]+)/,
  );
  assert.ok(m, "could not read the advertiser backoff ladder out of DirectorSyncModule.swift");
  const [, fastCount, base, factor, cap, lastResort] = m.map(Number);
  return { fastCount, base, factor, cap, lastResort };
};

const L = ladder();
const delayFor = (count) =>
  count <= L.fastCount ? Math.min(L.base * Math.pow(L.factor, count - 1), L.cap) : L.lastResort;

test("the ladder the model replays is the one the Swift actually computes", () => {
  // Pins the shape too, so a change to the constants is a deliberate, visible edit rather than a
  // silent drift the model would happily follow into nonsense.
  assert.deepEqual(L, { fastCount: 5, base: 3, factor: 2, cap: 30, lastResort: 45 },
    "the backoff constants changed — update the expectations here and confirm the intent");
  // The browser must use the same ladder; the file's comment says "same as the advertiser".
  assert.match(MODULE, /browserFailureCount <= 5\s*\?\s*min\(3\.0 \* pow\(2\.0, Double\(self\.browserFailureCount - 1\)\), 30\.0\)\s*:\s*45\.0/,
    "the browser ladder has drifted from the advertiser's");
});

/**
 * Replay a transport that fails immediately on every attempt.
 * `invalidateOnFailure` models whether a failure cancels the settle its attempt scheduled.
 */
const replay = ({ invalidateOnFailure, settleSeconds = 10, rounds = 8 }) => {
  let count = 0;
  let token = 0;
  let settle = null;
  const counts = [];
  const attempt = (at) => {
    token += 1;
    settle = { at: at + settleSeconds, token };
  };
  const fail = (at) => {
    count += 1;
    if (invalidateOnFailure) token += 1;
    counts.push(count);
    return at + delayFor(count);
  };
  attempt(0);
  let next = fail(0.1);
  for (let i = 0; i < rounds; i++) {
    if (settle && settle.at < next && settle.token === token) {
      count = 0; // the settle fired and declared the transport healthy
      settle = null;
    }
    attempt(next);
    next = fail(next + 0.1);
  }
  return counts;
};

test("a failed attempt cancels the settle it scheduled, or the ladder sawtooths forever", () => {
  const broken = replay({ invalidateOnFailure: false });
  assert.ok(Math.max(...broken) <= 3,
    "the pre-fix model should stall at the third rung — if not, this test no longer models the bug");

  const fixed = replay({ invalidateOnFailure: true });
  assert.ok(Math.max(...fixed) > 5,
    "the failure count still never exceeds 5 — the '> 5' foreground permission-recovery branch stays dead code");
  assert.deepEqual(fixed.slice(0, 7), [1, 2, 3, 4, 5, 6, 7], "the ladder no longer climbs monotonically");
});

test("the ladder reaches the 45 s last-resort tier and stays there", () => {
  const counts = replay({ invalidateOnFailure: true });
  const delays = counts.map(delayFor);
  assert.deepEqual(delays.slice(0, 6), [3, 6, 12, 24, 30, 45], "the documented ladder is not what is computed");
  assert.ok(delays.slice(6).every((d) => d === 45), "the last-resort tier is not sustained");
});

test("the counter is reset by a transport that SURVIVES, never by launching one", () => {
  // The original bug: startAdvertising/startBrowsing zeroed the counter, and the retry path calls
  // straight back into them.
  const adv = MODULE.slice(MODULE.indexOf("private func startAdvertising"), MODULE.indexOf("/// How long a transport must stay up"));
  assert.doesNotMatch(adv, /advertiserFailureCount = 0/,
    "startAdvertising zeroes the counter again — the retry path will reset it before it can climb");
  assert.match(adv, /noteTransportSettled\(advertiser: true\)/, "no settle scheduled for the advertiser");

  const brw = MODULE.slice(MODULE.indexOf("private func startBrowsing"), MODULE.indexOf("// Adaptive discovery refresh"));
  assert.doesNotMatch(brw, /browserFailureCount = 0/, "startBrowsing zeroes the counter again");
  assert.match(brw, /noteTransportSettled\(advertiser: false\)/, "no settle scheduled for the browser");

  // The reset belongs to the settle, and only the settle.
  const settleStart = MODULE.indexOf("private func noteTransportSettled");
  const settle = MODULE.slice(settleStart, MODULE.indexOf("private func startBrowsing", settleStart));
  assert.ok(settle.length > 0, "could not bound noteTransportSettled");
  assert.match(settle, /advertiserFailureCount = 0/, "the settle no longer resets the advertiser counter");
  assert.match(settle, /browserFailureCount = 0/, "the settle no longer resets the browser counter");
});

test("both didNotStart handlers invalidate their pending settle", () => {
  for (const [marker, end, flag] of [
    ["didNotStartAdvertisingPeer error", "// MARK: - MCNearbyServiceBrowserDelegate", "true"],
    ["didNotStartBrowsingForPeers error", "// MARK: - MCSessionDelegate", "false"],
  ]) {
    const body = MODULE.slice(MODULE.indexOf(marker), MODULE.indexOf(end, MODULE.indexOf(marker)));
    assert.ok(body.length > 0, `could not bound ${marker}`);
    assert.match(body, new RegExp(`invalidatePendingSettle\\(advertiser: ${flag}\\)`),
      `${marker} leaves its settle armed — the counter will be zeroed mid-wait`);
  }
});

test("both didNotStart handlers run on the main queue", () => {
  // They mutate the failure counters and read currentRole, which every other delegate callback in
  // this file touches only on main.
  for (const [marker, end] of [
    ["func advertiser(_ advertiser: MCNearbyServiceAdvertiser, didNotStartAdvertisingPeer", "// MARK: - MCNearbyServiceBrowserDelegate"],
    ["func browser(_ browser: MCNearbyServiceBrowser, didNotStartBrowsingForPeers", "// MARK: - MCSessionDelegate"],
  ]) {
    const start = MODULE.indexOf(marker);
    assert.ok(start > 0, `${marker} is gone`);
    const body = MODULE.slice(start, MODULE.indexOf(end, start));
    const brace = body.indexOf("{");
    assert.match(body.slice(brace, brace + 400), /DispatchQueue\.main\.async/,
      `${marker} mutates shared state off MPC's delegate queue`);
  }
});
